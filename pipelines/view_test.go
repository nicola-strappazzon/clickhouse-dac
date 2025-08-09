package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestView_Drop_DataBaseNameIsEmpty(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name(""),
		},
		View: pipelines.View{
			Name: pipelines.Name("bar"),
		},
	}
	v.SetParents()

	assert.Empty(t, v.View.DML())
	assert.Empty(t, v.View.Drop().DML())
}

func TestView_Drop_TableNameIsEmpty(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name: pipelines.Name(""),
		},
	}
	v.SetParents()

	assert.Empty(t, v.View.DML())
	assert.Empty(t, v.View.Drop().DML())
}

func TestView_Drop(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name: pipelines.Name("bar"),
		},
	}
	v.SetParents()

	assert.Empty(t, v.View.DML())
	assert.Equal(t, "DROP VIEW IF EXISTS foo.bar", v.View.Drop().DML())
}

func TestView_Create_Native(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name:  pipelines.Name("bar"),
			Query: "SELECT now()",
		},
	}
	v.SetParents()

	assert.Equal(t, "CREATE VIEW IF NOT EXISTS foo.bar AS SELECT now()", v.View.Create().DML())
}
