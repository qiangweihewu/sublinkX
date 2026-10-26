package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sublink/models"
	"sublink/node"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

const (
	directSS     = "ss://YWVzLTI1Ni1nY206cGFzcw@1.1.1.1:8388#direct"
	upstreamSS   = "ss://YWVzLTI1Ni1nY206cGFzcw@2.2.2.2:8388#up1"
	upstreamVLES = "vless://uuid@3.3.3.3:443?security=tls&alpn=h2,http/1.1#up2"
)

func setupSub(t *testing.T, name string, links ...string) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&models.Subcription{}, &models.SubLogs{}, &models.GroupNode{}, &models.Node{}); err != nil {
		t.Fatal(err)
	}
	models.DB = db
	config, _ := json.Marshal(node.SqlConfig{Clash: "../template/clash.yaml", Surge: "../template/surge.conf"})
	sub := models.Subcription{Name: name, Config: string(config)}
	for i, l := range links {
		sub.Nodes = append(sub.Nodes, models.Node{Name: "n" + string(rune('a'+i)), Link: l})
	}
	if err := sub.Add(); err != nil {
		t.Fatal(err)
	}
}

func request(token, ua, query string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/c/", GetClient)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/c/?token="+token+query, nil)
	req.Header.Set("User-Agent", ua)
	r.ServeHTTP(w, req)
	return w
}

func TestGetClientSkipsFailedUpstream(t *testing.T) {
	resetUpstream()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Subscription-Userinfo", "upload=1; download=2; total=3")
		// 带结尾换行的 base64，以前会解码失败
		w.Write([]byte(node.Base64Encode(upstreamSS+"\n"+upstreamVLES) + "\r\n"))
	}))
	defer ok.Close()
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("<!DOCTYPE html><title>Just a moment...</title>"))
	}))
	defer blocked.Close()

	setupSub(t, "测试", directSS, ok.URL+"/sub", blocked.URL+"/sub", "http://127.0.0.1:1/dead")
	token := Md5("测试")

	w := request(token, "Shadowrocket/2070 CFNetwork/1568 Darwin/24.0.0", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := node.Base64Decode(w.Body.String())
	for _, want := range []string{directSS, upstreamSS, upstreamVLES} {
		if !strings.Contains(got, want) {
			t.Errorf("v2ray 输出缺少 %s\n%s", want, got)
		}
	}
	if w.Header().Get("Subscription-Userinfo") == "" {
		t.Error("没有透传 Subscription-Userinfo")
	}

	w = request(token, "Shadowrocket", "&debug=1")
	if !strings.Contains(w.Body.String(), "403 Forbidden") || !strings.Contains(w.Body.String(), "节点总数: 3") {
		t.Errorf("debug 输出不对:\n%s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), blocked.URL) {
		t.Error("debug 输出泄露了上游链接")
	}

	w = request(token, "clash-verge/v1.7.7", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "proxies:") {
		t.Errorf("clash status=%d body=%.200s", w.Code, w.Body.String())
	}
}

func TestGetClientAllUpstreamFailed(t *testing.T) {
	resetUpstream()
	setupSub(t, "dead", "http://127.0.0.1:1/dead")
	w := request(Md5("dead"), "Shadowrocket", "")
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want 502", w.Code)
	}
	if w := request(Md5("nope"), "Shadowrocket", ""); w.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", w.Code)
	}
}

func TestGetClientFallsBackToStaleCache(t *testing.T) {
	resetUpstream()
	dead := "http://127.0.0.1:1/dead"
	upstreamCache[dead] = upstreamEntry{links: []string{upstreamSS}, at: time.Now().Add(-time.Hour)}
	setupSub(t, "stale", dead)
	w := request(Md5("stale"), "Shadowrocket", "")
	if w.Code != http.StatusOK || !strings.Contains(node.Base64Decode(w.Body.String()), upstreamSS) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func resetUpstream() {
	upstreamCache = map[string]upstreamEntry{}
	upstreamFailures = map[string]upstreamFailure{}
}

func TestSplitLinks(t *testing.T) {
	got := splitLinks(directSS + "," + upstreamVLES + "\n" + upstreamSS + ",")
	if len(got) != 3 || got[1] != upstreamVLES {
		t.Fatalf("got %q", got)
	}
}
