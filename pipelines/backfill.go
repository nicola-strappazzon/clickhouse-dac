package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

func (p Pipelines) Backfill() Pipelines {
	if !p.View.Materialized {
		return p
	}

	if p.View.Populate.Skip {
		return p
	}

	if !p.View.Populate.Type.IsBackFill() {
		return p
	}

	if p.Database.Name.IsEmpty() {
		return p
	}

	if p.View.To.IsEmpty() {
		return p
	}

	p.Statement = strings.Builder{}
	p.Statement.WriteString("INSERT INTO ")
	p.Statement.WriteString(p.Database.Name.ToString())
	p.Statement.WriteString(".")
	p.Statement.WriteString(p.View.To.ToString())

	if p.View.Columns.IsNotEmpty() {
		p.Statement.WriteString(" (")
		p.Statement.WriteString(p.View.Columns.WithoutTypes())
		p.Statement.WriteString(") ")
	} else if p.Table.Columns.IsNotEmpty() {
		p.Statement.WriteString(" (")
		p.Statement.WriteString(p.Table.Columns.WithoutTypes())
		p.Statement.WriteString(") ")
	}

	p.Statement.WriteString(p.View.Query.Minify())

	return p
}

func (p Pipelines) DML() string {
	return p.Statement.String()
}
