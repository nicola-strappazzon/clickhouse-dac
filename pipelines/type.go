package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type PopulateType string

const (
	PopulateNative   PopulateType = "native"   // Implemented, means: CREATE MATERIALIZED VIEW ... POPULATE AS SELECT ...
	PopulateBackFill PopulateType = "backfill" // Implemented, means: INSERT ... AS SELECT ...
	PopulateChunk    PopulateType = "chunk"    // Pending, populate via partition. INSERT ... AS SELECT ... WHERE (PARTITION BY)
)

type Populate struct {
	// CutOff string       `yaml:"cutoff"`
	Skip bool         `yaml:"skip"`
	Type PopulateType `yaml:"type"`
}

func (p PopulateType) ToString() string {
	return string(p)
}

func (p PopulateType) IsEmpty() bool {
	return strings.IsEmpty(p.ToString())
}

func (p PopulateType) IsNative() bool {
	return p == PopulateNative
}

func (p PopulateType) IsBackFill() bool {
	return p == PopulateBackFill
}
