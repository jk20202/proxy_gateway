package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"proxy-pool/internal/model"
	"proxy-pool/internal/pool"
	"proxy-pool/internal/provider"
)

func TestHTTPLatencyEndToEnd(t *testing.T) {
	proxies := make([]*model.Proxy, 0, 200)
	for i := range 200 {
		proxies = append(proxies, newTunnelProxy(fmt.Sprintf("p%d", i)))
	}
	p := pool.NewPool()
	for _, pr := range proxies {
		p.Add(pr)
	}
	mgr := &mockManager{provs: map[string]provider.Provider{"mock": mockProvider{kind: model.KindTunnel}}}
	s := NewWithPool(p, mgr, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := &fasthttp.Server{Handler: s.Handler(), Concurrency: 8192}
	go func() { _ = srv.Serve(ln) }()
	addr := ln.Addr().String()

	client := fasthttp.Client{
		MaxConnsPerHost: 512,
		ReadTimeout:     5 * time.Second,
	}

	const workers = 128
	const perWorker = 2000
	var success int64
	start := time.Now()

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)

			for range perWorker {
				req.SetRequestURI("http://" + addr + "/api/v1/proxy")
				if err := client.Do(req, resp); err != nil {
					continue
				}
				if resp.StatusCode() == fasthttp.StatusOK {
					atomic.AddInt64(&success, 1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	reqs := int64(workers) * perWorker
	rps := float64(reqs) / elapsed.Seconds()

	t.Logf("http: %d reqs in %v (%.0f rps), avg %.2f us/req, success=%d",
		reqs, elapsed, rps, float64(elapsed.Microseconds())/float64(reqs), success)

	var out struct {
		Proxy proxyResponse `json:"proxy"`
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("http://" + addr + "/api/v1/proxy")
	_ = client.Do(req, resp)
	_ = json.Unmarshal(resp.Body(), &out)
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)

	if out.Proxy.ID == "" {
		t.Fatal("proxy response invalid")
	}
}
