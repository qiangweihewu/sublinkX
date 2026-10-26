package api

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sublink/models"
	"sublink/node"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// 拉取上游订阅的参数
const (
	upstreamTimeout  = 10 * time.Second
	upstreamUA       = "v2rayN/6.45" // 用 v2rayN 的 UA，让机场面板返回 base64 节点列表
	upstreamMaxBody  = 10 << 20
	upstreamFreshTTL = 5 * time.Minute // 缓存有效期内直接用缓存，不再请求上游
	upstreamStaleTTL = 24 * time.Hour  // 上游失败时，可以兜底使用的旧缓存时长
	upstreamCooldown = time.Minute     // 上游失败后这段时间内不再重试，避免每个请求都卡到超时
)

var upstreamClient = &http.Client{Timeout: upstreamTimeout}

type upstreamEntry struct {
	links    []string
	userinfo string
	at       time.Time
}

type upstreamFailure struct {
	err error
	at  time.Time
}

var (
	upstreamMu       sync.Mutex
	upstreamCache    = map[string]upstreamEntry{}
	upstreamFailures = map[string]upstreamFailure{}
)

// 单个上游订阅的拉取结果，用于日志和 debug 输出
type upstreamReport struct {
	Node    string
	Host    string
	Status  string
	Count   int
	Elapsed time.Duration
	Err     string
}

// 客户端 UA 关键字 → 输出格式；都没匹配上的按 v2ray(base64) 输出，Shadowrocket、Quantumult X、Loon、v2rayN 等走这里
var clientUAKeywords = []struct{ keyword, client string }{
	{"clash", "clash"},
	{"mihomo", "clash"},
	{"stash", "clash"},
	{"surge", "surge"},
}

var schemeReg = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*://`)

// md5加密
func Md5(src string) string {
	m := md5.New()
	m.Write([]byte(src))
	res := hex.EncodeToString(m.Sum(nil))
	return res
}

func GetClient(c *gin.Context) {
	start := time.Now()
	rid := newRequestID()
	c.Header("X-Request-Id", rid)
	ua := c.GetHeader("User-Agent")
	ip := c.ClientIP()

	token := strings.ToLower(strings.TrimSpace(c.Query("token")))
	if token == "" {
		log.Printf("[订阅][%s] token为空 IP=%s UA=%q", rid, ip, ua)
		c.String(http.StatusBadRequest, "token为空")
		return
	}
	sub, err := findSubByToken(token)
	if err != nil {
		log.Printf("[订阅][%s] 找不到订阅 token=%s IP=%s UA=%q: %v", rid, token, ip, ua, err)
		c.String(http.StatusNotFound, "订阅不存在或token无效 (request id: %s)", rid)
		return
	}
	c.Set("subname", sub.Name)

	client := detectClient(c.Query("client"), ua)
	log.Printf("[订阅][%s] 请求 订阅=%s 客户端=%s client参数=%q IP=%s UA=%q", rid, sub.Name, client, c.Query("client"), ip, ua)

	links, reports, userinfo := collectLinks(rid, sub)
	failed, stale := 0, 0 // 失败且跳过的上游数、失败但用旧缓存兜底的上游数
	for _, r := range reports {
		switch {
		case r.Err != "" && r.Count == 0:
			failed++
		case r.Err != "":
			stale++
		}
	}

	if c.Query("debug") == "1" {
		writeDebug(c, rid, sub, client, ua, links, reports, start)
		return
	}

	if len(links) == 0 {
		if failed > 0 {
			log.Printf("[订阅][%s] 失败 订阅=%s 没有可用节点，%d 个上游订阅拉取失败 耗时=%s", rid, sub.Name, failed, time.Since(start))
			c.String(http.StatusBadGateway, "上游订阅全部拉取失败，没有可用节点 (request id: %s)", rid)
			return
		}
		log.Printf("[订阅][%s] 警告 订阅=%s 没有任何节点", rid, sub.Name)
	}
	if userinfo != "" {
		c.Header("Subscription-Userinfo", userinfo)
	}

	var size int
	switch client {
	case "clash":
		size, err = renderClash(c, sub, links)
	case "surge":
		size, err = renderSurge(c, sub, links)
	default:
		size = renderV2ray(c, sub, links)
	}
	if err != nil {
		log.Printf("[订阅][%s] 失败 订阅=%s 生成%s配置出错: %v", rid, sub.Name, client, err)
		c.String(http.StatusInternalServerError, "生成配置失败: %v (request id: %s)", err, rid)
		return
	}
	log.Printf("[订阅][%s] 完成 订阅=%s 客户端=%s 节点=%d 上游=%d(跳过%d 缓存兜底%d) 大小=%dB 耗时=%s",
		rid, sub.Name, client, len(links), len(reports), failed, stale, size, time.Since(start).Round(time.Millisecond))
}

// 根据 token(订阅名的md5) 找订阅
func findSubByToken(token string) (*models.Subcription, error) {
	var names []string
	if err := models.DB.Model(&models.Subcription{}).Pluck("name", &names).Error; err != nil {
		return nil, err
	}
	for _, name := range names {
		if Md5(name) == token {
			sub := &models.Subcription{Name: name}
			if err := sub.Find(); err != nil {
				return nil, err
			}
			return sub, nil
		}
	}
	return nil, errors.New("没有匹配的订阅")
}

// 判断输出格式：优先 client 参数，其次 UA
func detectClient(param, ua string) string {
	switch param {
	case "clash", "surge", "v2ray":
		return param
	}
	lower := strings.ToLower(ua)
	for _, k := range clientUAKeywords {
		if strings.Contains(lower, k.keyword) {
			return k.client
		}
	}
	return "v2ray"
}

// 汇总订阅里的所有节点链接；上游订阅并发拉取，单个失败只跳过它，不影响其他节点
func collectLinks(rid string, sub *models.Subcription) ([]string, []upstreamReport, string) {
	type part struct {
		link     string
		upstream bool
		links    []string
		userinfo string
		report   upstreamReport
	}
	var parts []part
	for _, n := range sub.Nodes {
		for _, link := range splitLinks(n.Link) {
			parts = append(parts, part{link: link, upstream: isHTTPURL(link), report: upstreamReport{Node: n.Name}})
		}
	}

	var wg sync.WaitGroup
	for i := range parts {
		if !parts[i].upstream {
			parts[i].links = []string{parts[i].link}
			continue
		}
		wg.Add(1)
		go func(p *part) {
			defer wg.Done()
			p.links, p.userinfo = fetchUpstream(rid, p.link, &p.report)
		}(&parts[i])
	}
	wg.Wait()

	links := []string{}
	var reports []upstreamReport
	var userinfos []string
	for _, p := range parts {
		links = append(links, p.links...)
		if p.upstream {
			reports = append(reports, p.report)
			if p.userinfo != "" {
				userinfos = append(userinfos, p.userinfo)
			}
		}
	}
	// 只有一个上游带流量信息时才透传，多个上游没法合并
	userinfo := ""
	if len(userinfos) == 1 {
		userinfo = userinfos[0]
	}
	return links, reports, userinfo
}

// 一个节点里可能存了多条链接(逗号或换行分隔)；只在逗号后紧跟 xxx:// 时才切分，避免把 alpn=h2,http/1.1 这类参数切断
func splitLinks(raw string) []string {
	var out []string
	for _, line := range strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == '\r' }) {
		start := len(out)
		for _, piece := range strings.Split(line, ",") {
			t := strings.TrimSpace(piece)
			if t == "" {
				continue
			}
			if len(out) > start && !schemeReg.MatchString(t) {
				out[len(out)-1] += "," + piece
				continue
			}
			out = append(out, t)
		}
	}
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out
}

func isHTTPURL(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// 拉取上游订阅：先看缓存，失败时用旧缓存兜底
func fetchUpstream(rid, link string, rep *upstreamReport) ([]string, string) {
	start := time.Now()
	rep.Host = hostOf(link)
	defer func() { rep.Elapsed = time.Since(start) }()

	upstreamMu.Lock()
	cached, ok := upstreamCache[link]
	failure, failed := upstreamFailures[link]
	upstreamMu.Unlock()
	if ok && time.Since(cached.at) < upstreamFreshTTL {
		rep.Status = "缓存"
		rep.Count = len(cached.links)
		log.Printf("[订阅][%s] 上游 %s(%s) 使用缓存 节点=%d 缓存于%s前", rid, rep.Node, rep.Host, rep.Count, time.Since(cached.at).Round(time.Second))
		return cached.links, cached.userinfo
	}

	var links []string
	var userinfo, status string
	var err error
	if failed && time.Since(failure.at) < upstreamCooldown {
		status = "冷却中"
		err = fmt.Errorf("%s前失败过，冷却中暂不重试: %w", time.Since(failure.at).Round(time.Second), failure.err)
	} else {
		links, userinfo, status, err = requestUpstream(link)
		upstreamMu.Lock()
		if err != nil {
			upstreamFailures[link] = upstreamFailure{err: err, at: time.Now()}
		} else {
			delete(upstreamFailures, link)
		}
		upstreamMu.Unlock()
	}
	rep.Status = status
	if err == nil {
		upstreamMu.Lock()
		upstreamCache[link] = upstreamEntry{links: links, userinfo: userinfo, at: time.Now()}
		upstreamMu.Unlock()
		rep.Count = len(links)
		log.Printf("[订阅][%s] 上游 %s(%s) 成功 %s 节点=%d 耗时=%s", rid, rep.Node, rep.Host, status, rep.Count, time.Since(start).Round(time.Millisecond))
		return links, userinfo
	}

	rep.Err = err.Error()
	if ok && time.Since(cached.at) < upstreamStaleTTL {
		rep.Count = len(cached.links)
		log.Printf("[订阅][%s] 上游 %s(%s) 失败: %v，改用%s前的缓存 节点=%d", rid, rep.Node, rep.Host, err, time.Since(cached.at).Round(time.Second), rep.Count)
		return cached.links, cached.userinfo
	}
	log.Printf("[订阅][%s] 上游 %s(%s) 失败: %v (无可用缓存，已跳过) 耗时=%s", rid, rep.Node, rep.Host, err, time.Since(start).Round(time.Millisecond))
	return nil, ""
}

// 请求上游订阅，返回节点链接、流量信息头、HTTP状态
func requestUpstream(link string) ([]string, string, string, error) {
	req, err := http.NewRequest(http.MethodGet, link, nil)
	if err != nil {
		return nil, "", "", fmt.Errorf("链接无效: %w", stripURLError(err))
	}
	req.Header.Set("User-Agent", upstreamUA)
	resp, err := upstreamClient.Do(req)
	if err != nil {
		// 去掉错误里的完整链接，避免把上游 token 打进日志和 debug 输出
		return nil, "", "", stripURLError(err)
	}
	defer resp.Body.Close()
	status := resp.Status
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamMaxBody))
	if err != nil {
		return nil, "", status, fmt.Errorf("读取响应失败: %w", stripURLError(err))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", status, fmt.Errorf("HTTP %s，内容: %s", status, snippet(body))
	}
	links, err := parseSubscription(body)
	if err != nil {
		return nil, "", status, err
	}
	return links, resp.Header.Get("Subscription-Userinfo"), status, nil
}

// 解析上游订阅内容：支持 base64 和明文链接列表
func parseSubscription(body []byte) ([]string, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, errors.New("上游返回内容为空(订阅可能已过期或被重置)")
	}
	if !strings.Contains(text, "://") {
		text = node.Base64Decode(text)
	}
	var links []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if schemeReg.MatchString(line) {
			links = append(links, line)
		}
	}
	if len(links) > 0 {
		return links, nil
	}
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "<html") || strings.Contains(lower, "<!doctype"):
		return nil, fmt.Errorf("上游返回了网页而不是订阅(可能被 Cloudflare/防火墙拦截): %s", snippet(body))
	case strings.Contains(lower, "proxies:"):
		return nil, errors.New("上游返回的是 Clash YAML，暂不支持，请换成 base64/v2ray 格式的订阅链接")
	}
	return nil, fmt.Errorf("上游返回内容无法识别为订阅: %s", snippet(body))
}

func renderV2ray(c *gin.Context, sub *models.Subcription, links []string) int {
	body := node.Base64Encode(strings.Join(links, "\n") + "\n")
	setFilename(c, sub.Name+".txt")
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(body))
	return len(body)
}

func renderClash(c *gin.Context, sub *models.Subcription, links []string) (int, error) {
	configs, err := subConfig(sub)
	if err != nil {
		return 0, err
	}
	out, err := node.EncodeClash(links, configs)
	if err != nil {
		return 0, err
	}
	setFilename(c, sub.Name+".yaml")
	c.Header("profile-update-interval", "24")
	c.Data(http.StatusOK, "text/plain; charset=utf-8", out)
	return len(out), nil
}

func renderSurge(c *gin.Context, sub *models.Subcription, links []string) (int, error) {
	configs, err := subConfig(sub)
	if err != nil {
		return 0, err
	}
	out, err := node.EncodeSurge(links, configs)
	if err != nil {
		return 0, err
	}
	// 如果不包含头部更新信息就插入，Surge 要求是带协议头的完整链接
	if !strings.Contains(out, "#!MANAGED-CONFIG") {
		out = fmt.Sprintf("#!MANAGED-CONFIG %s interval=86400 strict=false\n", requestURL(c)) + out
	}
	setFilename(c, sub.Name+".conf")
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(out))
	return len(out), nil
}

func subConfig(sub *models.Subcription) (node.SqlConfig, error) {
	var configs node.SqlConfig
	if err := json.Unmarshal([]byte(sub.Config), &configs); err != nil {
		return configs, fmt.Errorf("订阅配置读取错误: %w", err)
	}
	return configs, nil
}

// debug=1 时输出明文排查信息，不做 base64；不输出上游链接，避免泄露机场 token
func writeDebug(c *gin.Context, rid string, sub *models.Subcription, client, ua string, links []string, reports []upstreamReport, start time.Time) {
	var b strings.Builder
	fmt.Fprintf(&b, "request id: %s\n订阅: %s\n识别客户端: %s\nUA: %s\n节点总数: %d\n耗时: %s\n\n",
		rid, sub.Name, client, ua, len(links), time.Since(start).Round(time.Millisecond))
	fmt.Fprintf(&b, "上游订阅 (%d 个):\n", len(reports))
	for _, r := range reports {
		state := "成功"
		if r.Err != "" {
			state = "失败: " + r.Err
			if r.Count > 0 {
				state += " (已用缓存兜底)"
			}
		}
		fmt.Fprintf(&b, "- %s 状态=%s 节点=%d 耗时=%s %s\n", r.Node, r.Status, r.Count, r.Elapsed.Round(time.Millisecond), state)
	}
	b.WriteString("\n节点链接(明文):\n")
	b.WriteString(strings.Join(links, "\n"))
	b.WriteString("\n")
	log.Printf("[订阅][%s] debug输出 订阅=%s 节点=%d", rid, sub.Name, len(links))
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(b.String()))
}

func setFilename(c *gin.Context, filename string) {
	c.Header("Content-Disposition", "inline; filename*=utf-8''"+url.QueryEscape(filename))
}

// 还原客户端请求的完整链接，兼容反向代理
func requestURL(c *gin.Context) string {
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	if proto := c.GetHeader("X-Forwarded-Proto"); proto != "" {
		scheme = strings.TrimSpace(strings.Split(proto, ",")[0])
	}
	return scheme + "://" + c.Request.Host + c.Request.URL.RequestURI()
}

func hostOf(link string) string {
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return "无效链接"
	}
	return u.Host
}

func stripURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// 截取响应开头一小段，方便在日志里判断上游返回了什么
func snippet(body []byte) string {
	s := strings.Join(strings.Fields(string(body)), " ")
	if r := []rune(s); len(r) > 80 {
		return string(r[:80]) + "..."
	}
	return s
}

func newRequestID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return hex.EncodeToString(b)
}
