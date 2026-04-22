package jet

import "github.com/go-jet/jet/v2/internal/utils/is"

// ClauseStartWith is CUBRID's START WITH clause for hierarchical queries.
type ClauseStartWith struct {
	Condition BoolExpression
}

func (c *ClauseStartWith) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes START WITH clause into SQLBuilder.
func (c *ClauseStartWith) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.Condition == nil {
		return
	}
	out.NewLine()
	out.WriteString("START WITH")
	c.Condition.serialize(statementType, out, FallTrough(options)...)
}

// ClauseConnectBy is CUBRID's CONNECT BY clause for hierarchical queries.
type ClauseConnectBy struct {
	Condition BoolExpression
	NoCycle   bool
}

func (c *ClauseConnectBy) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes CONNECT BY clause into SQLBuilder.
func (c *ClauseConnectBy) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.Condition == nil {
		return
	}
	out.NewLine()
	if c.NoCycle {
		out.WriteString("CONNECT BY NOCYCLE")
	} else {
		out.WriteString("CONNECT BY")
	}
	c.Condition.serialize(statementType, out, FallTrough(options)...)
}

// ClauseOrderSiblingsBy is CUBRID's ORDER SIBLINGS BY clause.
type ClauseOrderSiblingsBy struct {
	List []OrderByClause
}

func (c *ClauseOrderSiblingsBy) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes ORDER SIBLINGS BY clause into SQLBuilder.
func (c *ClauseOrderSiblingsBy) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if len(c.List) == 0 {
		return
	}
	out.NewLine()
	out.WriteString("ORDER SIBLINGS BY")
	for i, value := range c.List {
		if i > 0 {
			out.WriteString(", ")
		}
		value.serializeForOrderBy(statementType, out)
	}
}

// ClauseMergeInto is CUBRID's MERGE INTO clause.
type ClauseMergeInto struct {
	Target SerializerTable
}

func (c *ClauseMergeInto) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes MERGE INTO clause into SQLBuilder.
func (c *ClauseMergeInto) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if is.Nil(c.Target) {
		return
	}
	out.NewLine()
	out.WriteString("MERGE INTO")
	c.Target.serialize(statementType, out)
}

// ClauseMergeUsing is CUBRID's USING clause for MERGE statement.
type ClauseMergeUsing struct {
	Source Serializer
}

func (c *ClauseMergeUsing) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes USING clause into SQLBuilder.
func (c *ClauseMergeUsing) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.Source == nil {
		return
	}
	out.NewLine()
	out.WriteString("USING")
	c.Source.serialize(statementType, out, FallTrough(options)...)
}

// ClauseMergeOn is CUBRID's ON clause for MERGE statement.
type ClauseMergeOn struct {
	Condition BoolExpression
}

func (c *ClauseMergeOn) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes ON clause into SQLBuilder.
func (c *ClauseMergeOn) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.Condition == nil {
		return
	}
	out.NewLine()
	out.WriteString("ON")
	c.Condition.serialize(statementType, out, FallTrough(options)...)
}

// ClauseWhenMatched is CUBRID's WHEN MATCHED THEN clause.
type ClauseWhenMatched struct {
	IsUpdate bool
	IsDelete bool
	Sets     []ColumnAssigment
}

func (c *ClauseWhenMatched) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes WHEN MATCHED clause into SQLBuilder.
func (c *ClauseWhenMatched) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if !c.IsUpdate && !c.IsDelete {
		return
	}
	if c.IsUpdate {
		out.NewLine()
		out.WriteString("WHEN MATCHED THEN UPDATE SET")
		for i, set := range c.Sets {
			if i > 0 {
				out.WriteString(",")
			}
			Serialize(set, statementType, out, FallTrough(options)...)
		}
	} else if c.IsDelete {
		out.NewLine()
		out.WriteString("WHEN MATCHED THEN DELETE")
	}
}

// ClauseWhenNotMatched is CUBRID's WHEN NOT MATCHED THEN INSERT clause.
type ClauseWhenNotMatched struct {
	Columns []Column
	Values  []interface{}
}

func (c *ClauseWhenNotMatched) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// Serialize serializes WHEN NOT MATCHED clause into SQLBuilder.
func (c *ClauseWhenNotMatched) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if len(c.Columns) == 0 {
		return
	}
	out.NewLine()
	out.WriteString("WHEN NOT MATCHED THEN INSERT (")
	for i, col := range c.Columns {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(col.Name())
	}
	out.WriteString(") VALUES (")
	for i, val := range c.Values {
		if i > 0 {
			out.WriteString(", ")
		}
		Serialize(Literal(val), statementType, out)
	}
	out.WriteString(")")
}

// ClauseReplaceInto is CUBRID's REPLACE INTO clause.
type ClauseReplaceInto struct {
	Table   SerializerTable
	Columns []Column
}

func (c *ClauseReplaceInto) serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	c.Serialize(statementType, out, options...)
}

// GetColumns returns the column list for REPLACE INTO.
func (c *ClauseReplaceInto) GetColumns() []Column {
	return c.Columns
}

// Serialize serializes REPLACE INTO clause into SQLBuilder.
func (c *ClauseReplaceInto) Serialize(statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	if is.Nil(c.Table) {
		panic("jet: table is nil for REPLACE INTO clause")
	}
	out.NewLine()
	out.WriteString("REPLACE INTO")
	c.Table.serialize(statementType, out)

	if len(c.Columns) == 0 {
		return
	}
	out.WriteString("(")
	SerializeColumnNames(c.Columns, out)
	out.WriteByte(')')
}
