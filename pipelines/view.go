package pipelines

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines/columns"
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type View struct {
	Columns      columns.Map     `yaml:"columns"`
	Delete       bool            `yaml:"delete"`
	Engine       string          `yaml:"engine"`
	Materialized bool            `yaml:"materialized"`
	Name         Name            `yaml:"name"`
	OrderBy      columns.Array   `yaml:"order_by"`
	PartitionBy  columns.Array   `yaml:"partition_by"`
	Populate     Populate        `yaml:"populate"`
	Query        Query           `yaml:"query"`
	Statement    strings.Builder `yaml:"-"`
	To           Name            `yaml:"to"`
	Parent       *Pipelines      `yaml:"-"`
}

func (v View) Drop() View {
	if v.Parent.Database.Name.IsEmpty() {
		return v
	}

	if v.Name.IsEmpty() {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("DROP VIEW IF EXISTS ")
	v.Statement.WriteString(v.Parent.Database.Name.ToString())
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name.ToString())

	return v
}

func (v View) Create() View {
	if v.Parent.Database.Name.IsEmpty() {
		return v
	}

	if v.Name.IsEmpty() {
		return v
	}

	if v.Parent.View.Query.IsEmpty() {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("CREATE ")

	if v.Materialized {
		v.Statement.WriteString("MATERIALIZED ")
	}

	v.Statement.WriteString("VIEW IF NOT EXISTS ")
	v.Statement.WriteString(v.Parent.Database.Name.ToString())
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name.ToString())

	if v.Materialized {
		if v.Populate.Type.IsBackFill() {
			v.Statement.WriteString(" TO ")
			v.Statement.WriteString(v.Parent.Database.Name.ToString())
			v.Statement.WriteString(".")
			v.Statement.WriteString(v.To.ToString())
		}

		if v.Populate.Type.IsNative() {
			if strings.IsNotEmpty(v.Engine) {
				v.Statement.WriteString(" ENGINE=")
				v.Statement.WriteString(v.Engine)
			}

			if v.PartitionBy.IsNotEmpty() {
				v.Statement.WriteString(" PARTITION BY (")
				v.Statement.WriteString(v.PartitionBy.Join())
				v.Statement.WriteString(")")
			}

			if v.OrderBy.IsNotEmpty() {
				v.Statement.WriteString(" ORDER BY (")
				v.Statement.WriteString(v.OrderBy.Join())
				v.Statement.WriteString(")")
			}

			v.Statement.WriteString(" POPULATE")
		}

		if v.Columns.IsNotEmpty() {
			v.Statement.WriteString(" (")
			v.Statement.WriteString(v.Columns.WithTypes())
			v.Statement.WriteString(")")
		}
	}

	v.Statement.WriteString(" AS ")
	v.Statement.WriteString(v.Query.Minify())

	return v
}

func (v View) DML() string {
	return v.Statement.String()
}

func (v View) Validate() error {
	var re = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{1,254}$`)

	if reflect.DeepEqual(v, View{}) {
		return nil
	}

	if v.Name.IsEmpty() {
		return fmt.Errorf("view.name is required")
	}

	if !re.MatchString(v.Name.ToString()) {
		return fmt.Errorf("view.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", v.Name.ToString())
	}

	if v.Query.IsEmpty() {
		return fmt.Errorf("view.query is required")
	}

	if v.Name.IsEmpty() && v.Delete {
		return fmt.Errorf("view.name is required")
	}

	if v.Materialized && v.Populate.Type.IsEmpty() {
		return fmt.Errorf("view.populate.type is required")
	}

	if v.Populate.Type.IsBackFill() && v.To.IsEmpty() {
		return fmt.Errorf("view.to is required")
	}

	//

	return nil
}
