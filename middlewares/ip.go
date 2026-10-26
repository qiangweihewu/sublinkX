package middlewares

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sublink/models"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/text/encoding/simplifiedchinese"
)

var ipInfoClient = &http.Client{Timeout: 5 * time.Second}

func GetIp(c *gin.Context) {
	c.Next()
	// 只记录成功的订阅请求，debug 请求不计入
	subname, _ := c.Get("subname")
	name, ok := subname.(string)
	if !ok || name == "" || c.Writer.Status() != http.StatusOK || c.Query("debug") == "1" {
		return
	}
	ip := c.ClientIP()
	// 放到后台执行：查询 IP 归属地可能很慢，不能拖住订阅响应
	go recordAccess(name, ip)
}

func recordAccess(subname, ip string) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("[访问记录] panic:", r)
		}
	}()

	var sub models.Subcription
	sub.Name = subname
	if err := sub.Find(); err != nil {
		log.Printf("[访问记录] 查找订阅 %s 失败: %v", subname, err)
		return
	}

	var iplog models.SubLogs
	iplog.IP = ip
	// 查找是否存在该 IP 记录，没有就创建新记录
	if err := iplog.Find(sub.ID); err != nil {
		newIplog := models.SubLogs{
			IP:            ip,
			Addr:          lookupIPAddr(ip),
			SubcriptionID: sub.ID,
			Date:          time.Now().Format("2006-01-02 15:04:05"),
			Count:         1,
		}
		if err := newIplog.Add(); err != nil {
			log.Printf("[访问记录] 订阅=%s IP=%s 添加记录失败: %v", subname, ip, err)
			return
		}
		log.Printf("[访问记录] 订阅=%s IP=%s 归属=%s 新增记录", subname, ip, newIplog.Addr)
		return
	}
	// 找到了记录就更新访问次数和日期
	iplog.Count++
	iplog.Date = time.Now().Format("2006-01-02 15:04:05")
	if iplog.Addr == "" {
		iplog.Addr = lookupIPAddr(ip)
	}
	if err := iplog.Update(); err != nil {
		log.Printf("[访问记录] 订阅=%s IP=%s 更新记录失败: %v", subname, ip, err)
	}
}

// 查询 IP 归属地，失败时返回空字符串，不影响访问记录
func lookupIPAddr(ip string) string {
	resp, err := ipInfoClient.Get(fmt.Sprintf("https://whois.pconline.com.cn/ipJson.jsp?ip=%s&json=true", ip))
	if err != nil {
		log.Println("[访问记录] 获取IP信息失败:", err)
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	utf8Body, _ := simplifiedchinese.GBK.NewDecoder().Bytes(body)
	type IpInfo struct {
		Addr string `json:"addr"`
		Ip   string `json:"ip"`
	}
	ipinfo := IpInfo{}
	if err := json.Unmarshal(utf8Body, &ipinfo); err != nil {
		log.Println("[访问记录] 解析IP信息失败:", err)
		return ""
	}
	return ipinfo.Addr
}
