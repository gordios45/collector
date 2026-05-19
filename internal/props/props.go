// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package props contains helpers for loosely typed JSON/property maps.
package props

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func Float(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case []byte:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(x)), 64)
		return f, err == nil
	case string:
		var f float64
		if _, err := fmt.Sscanf(strings.TrimSpace(x), "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

func Int(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case int32:
		return int(x), true
	case float64:
		return int(x), true
	case float32:
		return int(x), true
	case json.Number:
		n, err := strconv.Atoi(x.String())
		return n, err == nil
	case []byte:
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(string(x)), "%d", &n); err == nil {
			return n, true
		}
	case string:
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(x), "%d", &n); err == nil {
			return n, true
		}
	}
	return 0, false
}

func StringAt(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

func FirstNonEmpty(xs ...string) string {
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x != "" {
			return x
		}
	}
	return ""
}

func StringList(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			s := strings.TrimSpace(fmt.Sprint(item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return nil
		}
		parts := strings.Split(x, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(strings.Trim(strings.TrimSpace(part), "[]\"'"))
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	default:
		return nil
	}
}

func SetNonEmpty(m map[string]any, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		m[key] = value
	}
}

func ClampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
