// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// httpx — tiny helpers shared across collectors. Fetch JSON/CSV with timeout,
// sane defaults, and simple 429/5xx error wrapping.
package httpx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"
)

var defaultClient = &http.Client{Timeout: 20 * time.Second}

const defaultUA = "gordios/0.1 (+https://github.com/gordios)"

// GetJSON does GET url, decodes JSON into out. Optional header map.
func GetJSON(ctx context.Context, url string, headers map[string]string, out any) error {
	return getWith(ctx, defaultClient, url, headers, out, true)
}

// GetJSONWithClient is GetJSON using a caller-provided client.
func GetJSONWithClient(ctx context.Context, client *http.Client, url string, headers map[string]string, out any) error {
	return getWith(ctx, client, url, headers, out, true)
}

// GetBytes does GET url and returns the raw body.
func GetBytes(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	return getBytesWith(ctx, defaultClient, url, headers)
}

// GetBytesWithClient is GetBytes using a caller-provided client.
func GetBytesWithClient(ctx context.Context, client *http.Client, url string, headers map[string]string) ([]byte, error) {
	return getBytesWith(ctx, client, url, headers)
}

// getWith is the shared code path; browser=true means utls client, false = default.
func getWith(ctx context.Context, client *http.Client, url string, headers map[string]string, out any, asJSON bool) error {
	buf, err := getBytesWith(ctx, client, url, headers)
	if err != nil {
		return err
	}
	if asJSON {
		return json.Unmarshal(buf, out)
	}
	return nil
}

func getBytesWith(ctx context.Context, client *http.Client, url string, headers map[string]string) ([]byte, error) {
	if client == nil {
		client = defaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET %q: %w", url, err)
	}
	req.Header.Set("User-Agent", defaultUA)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	r, err := client.Do(req)
	if err != nil {
		if buf, curlErr := curlGetBytes(ctx, url, headers); curlErr == nil {
			return buf, nil
		}
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 400))
		return nil, fmt.Errorf("%s → %d: %s", url, r.StatusCode, string(body))
	}
	return io.ReadAll(r.Body)
}

func curlGetBytes(ctx context.Context, rawURL string, headers map[string]string) ([]byte, error) {
	args := []string{"-fsSL", "--connect-timeout", "30", "--max-time", "120", "-A", defaultUA}
	for k, v := range headers {
		args = append(args, "-H", k+": "+v)
	}
	args = append(args, rawURL)
	return exec.CommandContext(ctx, "curl", args...).Output()
}
