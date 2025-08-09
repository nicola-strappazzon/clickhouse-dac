package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestPopulateType_ToString(t *testing.T) {
	cases := []struct {
		name   string
		input  pipelines.PopulateType
		expect string
	}{
		{"empty", "", ""},
		{"native", pipelines.PopulateNative, "native"},
		{"backfill", pipelines.PopulateBackFill, "backfill"},
		{"chunk", pipelines.PopulateChunk, "chunk"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.input.ToString())
		})
	}
}

func TestPopulateType_IsEmpty(t *testing.T) {
	cases := []struct {
		name  string
		input pipelines.PopulateType
		empty bool
	}{
		{"zero value", "", true},
		{"explicit empty", pipelines.PopulateType(""), true},
		{"native", pipelines.PopulateNative, false},
		{"backfill", pipelines.PopulateBackFill, false},
		{"chunk", pipelines.PopulateChunk, false},
		{"unknown non-empty", pipelines.PopulateType("something"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.empty, tc.input.IsEmpty())
		})
	}
}

func TestPopulateType_IsNative(t *testing.T) {
	cases := []struct {
		name   string
		input  pipelines.PopulateType
		expect bool
	}{
		{"native", pipelines.PopulateNative, true},
		{"backfill", pipelines.PopulateBackFill, false},
		{"chunk", pipelines.PopulateChunk, false},
		{"empty", "", false},
		{"unknown", pipelines.PopulateType("weird"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.input.IsNative())
		})
	}
}

func TestPopulateType_IsBackFill(t *testing.T) {
	cases := []struct {
		name   string
		input  pipelines.PopulateType
		expect bool
	}{
		{"backfill", pipelines.PopulateBackFill, true}, // ← Este test FALLARÁ con la implementación actual
		{"native", pipelines.PopulateNative, false},
		{"chunk", pipelines.PopulateChunk, false},
		{"empty", "", false},
		{"unknown", pipelines.PopulateType("weird"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.input.IsBackFill())
		})
	}
}
