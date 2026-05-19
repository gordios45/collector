// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"encoding/json"
	"strconv"
	"strings"
)

func nestedFloat64(m map[string]any, path string) (float64, bool) {
	if m == nil || path == "" {
		return 0, false
	}
	cur := any(m)
	for _, p := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return 0, false
		}
		cur, ok = obj[p]
		if !ok {
			return 0, false
		}
	}
	switch v := cur.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
