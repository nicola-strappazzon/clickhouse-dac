package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Name string

func (n Name) ToString() string {
	return string(n)
}

func (n Name) IsEmpty() bool {
	return strings.IsEmpty(n.ToString())
}

func (n Name) IsNotEmpty() bool {
	return !n.IsEmpty()
}
