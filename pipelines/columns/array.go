package columns

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Array []Name

func (c Array) ToArrayString() []string {
	r := make([]string, len(c))
	for i, val := range c {
		r[i] = val.ToString()
	}
	return r
}

func (c Array) Join() string {
	return strings.Join(c.ToArrayString())
}

func (c Array) IsEmpty() bool {
	return len(c) == 0
}

func (c Array) IsNotEmpty() bool {
	return !c.IsEmpty()
}
