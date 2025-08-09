package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestName_ToString(t *testing.T) {
	cases := []struct {
		name   string
		input  pipelines.Name
		expect string
	}{
		{"empty", pipelines.Name(""), ""},
		{"simple", pipelines.Name("foo"), "foo"},
		{"whitespace", pipelines.Name("  "), "  "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.input.ToString())
		})
	}
}

func TestName_IsEmpty(t *testing.T) {
	var zero pipelines.Name

	cases := []struct {
		name  string
		input pipelines.Name
		empty bool
	}{
		{"zero value", zero, true},
		{"empty literal", pipelines.Name(""), true},
		{"non empty", pipelines.Name("foo"), false},
		{"whitespace counts as empty", pipelines.Name(" "), true},
		{"unicode non empty", pipelines.Name("á"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.empty, tc.input.IsEmpty())
			assert.Equal(t, !tc.empty, tc.input.IsNotEmpty())
		})
	}
}
