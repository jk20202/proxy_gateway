package geo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLookupPublicIPs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		var ips []string
		if err := json.NewDecoder(r.Body).Decode(&ips); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode([]map[string]string{
			{"query": "8.8.8.8", "status": "success", "countryCode": "US"},
			{"query": "8.8.4.4", "status": "success", "countryCode": "US"},
		})
	}))
	defer srv.Close()

	c := New(srv.URL + "/batch")
	out := c.Lookup(context.Background(), []string{"8.8.8.8", "8.8.4.4", "8.8.8.8"})
	if out["8.8.8.8"] != "US" {
		t.Errorf("8.8.8.8 = %q, want US", out["8.8.8.8"])
	}
	if out["8.8.4.4"] != "US" {
		t.Errorf("8.8.4.4 = %q, want US", out["8.8.4.4"])
	}
}

func TestLookupSkipsPrivateAndCache(t *testing.T) {
	got := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = true
		var ips []string
		_ = json.NewDecoder(r.Body).Decode(&ips)
		_ = json.NewEncoder(w).Encode([]map[string]string{{"query": "1.2.3.4", "status": "success", "countryCode": "JP"}})
	}))
	defer srv.Close()

	c := New(srv.URL + "/batch")

	// private / loopback / empty are skipped without hitting the server
	out := c.Lookup(context.Background(), []string{"127.0.0.1", "10.0.0.1", "192.168.1.1", "::1", ""})
	if got {
		t.Fatal("server should not be called for private IPs")
	}
	for ip, country := range out {
		if country != "" {
			t.Errorf("private ip %s resolved to %q, want empty", ip, country)
		}
	}

	// a public IP is queried then cached
	c.Lookup(context.Background(), []string{"1.2.3.4"})
	if !got {
		t.Fatal("server should be called for a public IP")
	}
	if c.Country("1.2.3.4") != "JP" {
		t.Errorf("cached country = %q, want JP", c.Country("1.2.3.4"))
	}
}
