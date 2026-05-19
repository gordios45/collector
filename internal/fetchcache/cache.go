// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package fetchcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/httpx"
)

type BytesFetcher interface {
	GetBytes(ctx context.Context, url string, headers map[string]string) ([]byte, error)
}

type HTTPFetcher struct{}

func (HTTPFetcher) GetBytes(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	return httpx.GetBytes(ctx, url, headers)
}

type CachedFetcher struct {
	Dir     string
	Refresh bool
	Logf    func(format string, args ...any)
	Client  *http.Client
}

type metadata struct {
	URL          string    `json:"url"`
	ETag         string    `json:"etag,omitempty"`
	LastModified string    `json:"last_modified,omitempty"`
	FetchedAt    time.Time `json:"fetched_at"`
}

func (f CachedFetcher) GetBytes(ctx context.Context, rawURL string, headers map[string]string) ([]byte, error) {
	if f.Dir == "" {
		return httpx.GetBytes(ctx, rawURL, headers)
	}
	if err := os.MkdirAll(f.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir %s: %w", f.Dir, err)
	}

	bodyPath, metaPath := cachePaths(f.Dir, rawURL)
	body, hasBody := readBody(bodyPath)
	meta := readMetadata(metaPath)

	if hasBody && !f.Refresh && meta.ETag == "" && meta.LastModified == "" {
		f.logf("[download] %s has no ETag/Last-Modified; using cached copy. Pass -refresh or delete %s to re-download.", rawURL, bodyPath)
		return body, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET %q: %w", rawURL, err)
	}
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if !f.Refresh {
		if meta.ETag != "" {
			req.Header.Set("If-None-Match", meta.ETag)
		}
		if meta.LastModified != "" {
			req.Header.Set("If-Modified-Since", meta.LastModified)
		}
	}

	client := f.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Minute}
	}
	resp, err := client.Do(req)
	if err != nil {
		if hasBody {
			f.logf("[download] %s fetch failed (%v); using stale cached copy %s", rawURL, err, bodyPath)
			return body, nil
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		if hasBody {
			f.logf("[download] %s not modified; using cache %s", rawURL, bodyPath)
			return body, nil
		}
		return nil, fmt.Errorf("%s returned 304 but cache body is missing", rawURL)
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		if hasBody {
			f.logf("[download] %s returned %d; using stale cached copy %s", rawURL, resp.StatusCode, bodyPath)
			return body, nil
		}
		return nil, fmt.Errorf("%s -> %d: %s", rawURL, resp.StatusCode, string(msg))
	}

	fresh, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(bodyPath, fresh, 0o644); err != nil {
		return nil, fmt.Errorf("write cache %s: %w", bodyPath, err)
	}
	meta = metadata{
		URL:          rawURL,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		FetchedAt:    time.Now().UTC(),
	}
	if err := writeMetadata(metaPath, meta); err != nil {
		return nil, err
	}
	f.logf("[download] downloaded %s -> %s", rawURL, bodyPath)
	return fresh, nil
}

func (f CachedFetcher) logf(format string, args ...any) {
	if f.Logf != nil {
		f.Logf(format, args...)
	}
}

func cachePaths(dir, rawURL string) (string, string) {
	sum := sha256.Sum256([]byte(rawURL))
	hash := hex.EncodeToString(sum[:])[:12]
	name := sanitizeName(path.Base(rawURL))
	if name == "" || name == "." || name == "/" {
		name = "download"
	}
	base := filepath.Join(dir, name+"-"+hash)
	return base + ".body", base + ".json"
}

func sanitizeName(name string) string {
	name = strings.ToLower(name)
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return strings.Trim(b.String(), "-")
}

func readBody(p string) ([]byte, bool) {
	buf, err := os.ReadFile(p)
	return buf, err == nil
}

func readMetadata(p string) metadata {
	var meta metadata
	buf, err := os.ReadFile(p)
	if err == nil {
		_ = json.Unmarshal(buf, &meta)
	}
	return meta
}

func writeMetadata(p string, meta metadata) error {
	buf, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		return fmt.Errorf("write cache metadata %s: %w", p, err)
	}
	return nil
}
