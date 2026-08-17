// Package geo resolves an IP address to its ISO 3166-1 alpha-2 country code
// using the free ip-api.com batch endpoint. Private / loopback / link-local
// addresses are skipped (they cannot be geolocated) and known answers are
// cached to keep the free quota (15 batch req/min) from being exhausted.
package geo

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"
)

const defaultBatchURL = "http://ip-api.com/batch"

// Client resolves a batch of IP addresses to country codes.
type Client struct {
	url string
	hc  *http.Client

	mu    sync.Mutex
	cache map[string]string // ip -> country code
}

// New returns a Client that queries the given batch endpoint. An empty url
// uses the default ip-api.com/batch endpoint.
func New(url string) *Client {
	if url == "" {
		url = defaultBatchURL
	}
	return &Client{
		url:   url,
		hc:    &http.Client{Timeout: 10 * time.Second},
		cache: make(map[string]string),
	}
}

// isPublicIP reports whether the IP can be geolocated: not private, loopback,
// link-local, multicast or unspecified.
func isPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	return !(ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified())
}

// Country returns the cached country code for a single IP, or "" if unknown.
func (c *Client) Country(ip string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache[ip]
}

// Lookup resolves the given IPs to country codes. Private and non-public IPs
// are skipped. The returned map contains an entry for every queried IP; IPs
// that could not be resolved map to "".
func (c *Client) Lookup(ctx context.Context, ips []string) map[string]string {
	out := make(map[string]string, len(ips))

	// 1. filter to public IPs we don't already know.
	var pending []string
	seen := make(map[string]bool, len(ips))
	for _, ip := range ips {
		if ip == "" {
			out[ip] = ""
			continue
		}
		parsed := net.ParseIP(ip)
		if !isPublicIP(parsed) {
			out[ip] = ""
			continue
		}
		c.mu.Lock()
		country, ok := c.cache[ip]
		c.mu.Unlock()
		if ok {
			out[ip] = country
			continue
		}
		if seen[ip] {
			continue
		}
		seen[ip] = true
		pending = append(pending, ip)
	}
	if len(pending) == 0 {
		return out
	}

	// 2. batch query (ip-api accepts up to 100 IPs per batch request).
	var body bytes.Buffer
	_ = json.NewEncoder(&body).Encode(pending)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, &body)
	if err != nil {
		for _, ip := range pending {
			out[ip] = ""
		}
		return out
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		for _, ip := range pending {
			out[ip] = ""
		}
		return out
	}
	defer resp.Body.Close()

	var results []struct {
		Query       string `json:"query"`
		Status      string `json:"status"`
		CountryCode string `json:"countryCode"`
	}
	if resp.StatusCode != http.StatusOK {
		for _, ip := range pending {
			out[ip] = ""
		}
		return out
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		for _, ip := range pending {
			out[ip] = ""
		}
		return out
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range results {
		country := ""
		if r.Status == "success" {
			country = r.CountryCode
		}
		c.cache[r.Query] = country
		out[r.Query] = country
	}
	return out
}
