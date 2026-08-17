// 模拟代理供应商环境，用于端到端联调：
//  1. mock-tunnel-gateway：模拟隧道型代理网关（HTTP forward proxy）
//  2. mock-ip-proxies：模拟 IP 池型代理节点（部分存活、部分不可达）
//  3. mock-extract-api：模拟 IP 池提取 API
//  4. mock-target：模拟爬虫目标站点
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

var (
	gatewayPort = flag.Int("gateway-port", 10001, "mock tunnel gateway port")
	targetPort  = flag.Int("target-port", 10002, "mock target site port")
	apiPort     = flag.Int("api-port", 10003, "mock extract api port")
	adminPort   = flag.Int("admin-port", 10004, "mock admin port")
	ipPoolBase  = flag.Int("ip-pool-base", 10100, "base port for mock ip pool proxies")
	ipCount     = flag.Int("ip-count", 10, "number of mock ip pool proxies")
	gatewayFail atomic.Bool
)

func main() {
	flag.Parse()

	go runTarget()
	go runExtractAPI()
	go runGateway()
	go runAdmin()

	// mock ip-pool proxy nodes: ports ipPoolBase+i (odd offset live, even offset dead)
	for i := 1; i <= *ipCount; i++ {
		port := *ipPoolBase + i*2
		go runMockProxyNode(port, i%2 == 1)
	}
	log.Println("mock env ready")
	select {}
}

// admin api to simulate provider outages
func runAdmin() {
	mux := http.NewServeMux()
	mux.HandleFunc("/gateway/fail", func(w http.ResponseWriter, r *http.Request) {
		gatewayFail.Store(true)
		fmt.Fprintf(w, "gateway now failing\n")
	})
	mux.HandleFunc("/gateway/recover", func(w http.ResponseWriter, r *http.Request) {
		gatewayFail.Store(false)
		fmt.Fprintf(w, "gateway recovered\n")
	})
	mux.HandleFunc("/alert-webhook", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		log.Printf("[alert-webhook] %s", string(b))
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", *adminPort), Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func runTarget() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ip":"%s","via":"mock-target"}`, clientIP(r))
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", *targetPort), Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// mock extract api: returns a list of live ip:port entries
func runExtractAPI() {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		for i := 1; i <= *ipCount; i++ {
			port := *ipPoolBase + i*2
			fmt.Fprintf(w, "127.0.0.1:%d\n", port)
		}
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", *apiPort), Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// mock tunnel gateway: HTTP forward proxy
func runGateway() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *gatewayPort))
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go func() {
			handleProxyConn(conn, gatewayFail.Load())
		}()
	}
}

// mock ip-pool proxy node: same forward-proxy logic; alive controls availability
func runMockProxyNode(port int, alive bool) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("mock ip node %d failed: %v", port, err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go func() {
			handleProxyConn(conn, !alive)
		}()
	}
}

func handleProxyConn(client net.Conn, fail bool) {
	defer client.Close()
	br := bufio.NewReader(client)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}

	if fail {
		client.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
		return
	}

	if req.Method == http.MethodConnect {
		upstream, err := net.DialTimeout("tcp", req.Host, 5*time.Second)
		if err != nil {
			client.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
			return
		}
		defer upstream.Close()
		client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		done := make(chan struct{})
		go func() {
			io.Copy(upstream, client)
			if tc, ok := upstream.(*net.TCPConn); ok {
				tc.CloseWrite()
			}
			close(done)
		}()
		io.Copy(client, upstream)
		if tc, ok := client.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		<-done
		return
	}

	targetURL := req.URL
	if !targetURL.IsAbs() {
		targetURL = &url.URL{Scheme: "http", Host: req.Host, Path: req.URL.Path}
	}
	outReq, err := http.NewRequest(req.Method, targetURL.String(), req.Body)
	if err != nil {
		return
	}
	outReq.Header = req.Header.Clone()
	outReq.Header.Set("X-Forwarded-For", client.RemoteAddr().(net.Addr).String())

	resp, err := http.DefaultTransport.RoundTrip(outReq)
	if err != nil {
		client.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	defer resp.Body.Close()
	_ = resp.Write(client)
}
