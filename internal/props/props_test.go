// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package props

import (
	"encoding/json"
	"testing"
)

func TestFloatHandlesLooseJSONValues(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want float64
	}{
		{name: "float", in: 1.5, want: 1.5},
		{name: "int", in: 2, want: 2},
		{name: "json number", in: json.Number("3.25"), want: 3.25},
		{name: "string prefix", in: "4.5 km", want: 4.5},
		{name: "bytes", in: []byte("5.75"), want: 5.75},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Float(tt.in)
			if !ok || got != tt.want {
				t.Fatalf("Float(%#v) = %v, %v; want %v, true", tt.in, got, ok, tt.want)
			}
		})
	}
}

func TestIntHandlesLooseJSONValues(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want int
	}{
		{name: "int", in: 2, want: 2},
		{name: "float", in: 3.8, want: 3},
		{name: "json number", in: json.Number("4"), want: 4},
		{name: "string prefix", in: "5 days", want: 5},
		{name: "bytes", in: []byte("6"), want: 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Int(tt.in)
			if !ok || got != tt.want {
				t.Fatalf("Int(%#v) = %v, %v; want %v, true", tt.in, got, ok, tt.want)
			}
		})
	}
}

func TestStringAtFormatsPresentValues(t *testing.T) {
	m := map[string]any{"name": " event ", "count": 12}
	if got := StringAt(m, "name"); got != " event " {
		t.Fatalf("StringAt string = %q", got)
	}
	if got := StringAt(m, "count"); got != "12" {
		t.Fatalf("StringAt number = %q", got)
	}
	if got := StringAt(nil, "missing"); got != "" {
		t.Fatalf("StringAt nil = %q", got)
	}
}

func TestFirstNonEmptyTrimsResult(t *testing.T) {
	if got := FirstNonEmpty(" ", " ok ", "next"); got != "ok" {
		t.Fatalf("FirstNonEmpty = %q, want ok", got)
	}
}

func TestStringListNormalizesCommonShapes(t *testing.T) {
	got := StringList(`["flood", " storm ", ""]`)
	want := []string{"flood", "storm"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("item %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestClampFloatBoundsValue(t *testing.T) {
	if got := ClampFloat(-2, 0, 1); got != 0 {
		t.Fatalf("low clamp = %v", got)
	}
	if got := ClampFloat(2, 0, 1); got != 1 {
		t.Fatalf("high clamp = %v", got)
	}
	if got := ClampFloat(0.5, 0, 1); got != 0.5 {
		t.Fatalf("in range = %v", got)
	}
}

func TestSetNonEmptyTrimsValue(t *testing.T) {
	m := map[string]any{}
	SetNonEmpty(m, "empty", " ")
	SetNonEmpty(m, "value", " ok ")
	if _, ok := m["empty"]; ok {
		t.Fatal("empty value was set")
	}
	if m["value"] != "ok" {
		t.Fatalf("value = %#v, want ok", m["value"])
	}
}
