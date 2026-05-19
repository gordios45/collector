// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Browser-like TLS client using utls. Some APIs (FAA/Akamai, Cloudflare-bot,
// etc.) fingerprint the TLS ClientHello and reject Go's crypto/tls handshake.
// We forge a Chrome-120 hello so the server sees a browser-looking handshake.
package httpx

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	utls "github.com/refraction-networking/utls"
)

// browserClient is a lazily-built http.Client whose Transport dials TLS
// with utls.HelloChrome_Auto. Shared across collectors that need it.
var (
	browserClient     *http.Client
	browserClientOnce sync.Once
)

func BrowserClient() *http.Client {
	browserClientOnce.Do(func() {
		browserClient = newBrowserClient()
	})
	return browserClient
}

func newBrowserClient() *http.Client {
	tr := &http.Transport{
		// Pool connections per host, just like net/http defaults.
		MaxIdleConns:        32,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 15 * time.Second,
		// Key piece: DialTLSContext wraps the raw TCP conn in a utls handshake.
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				host = strings.SplitN(addr, ":", 2)[0]
			}
			rawConn, err := (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			cfg := &utls.Config{ServerName: host}
			u := utls.UClient(rawConn, cfg, utls.HelloChrome_Auto)
			if err := u.HandshakeContext(ctx); err != nil {
				_ = rawConn.Close()
				return nil, err
			}
			return u, nil
		},
	}
	return &http.Client{Transport: tr, Timeout: 25 * time.Second}
}

// GetJSONBrowser is like GetJSON but uses BrowserClient (utls Chrome hello).
func GetJSONBrowser(ctx context.Context, url string, headers map[string]string, out any) error {
	return getWith(ctx, BrowserClient(), url, headers, out, true)
}

// GetBytesBrowser is like GetBytes but uses BrowserClient.
func GetBytesBrowser(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	b, err := getBytesWith(ctx, BrowserClient(), url, headers)
	return b, err
}
