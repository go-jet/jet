package jet

import (
	"testing"
)

func TestClauseStartWith_Empty(t *testing.T) {
	c := &ClauseStartWith{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseStartWith should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseConnectBy_Empty(t *testing.T) {
	c := &ClauseConnectBy{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseConnectBy should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseOrderSiblingsBy_Empty(t *testing.T) {
	c := &ClauseOrderSiblingsBy{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseOrderSiblingsBy should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseMergeInto_Empty(t *testing.T) {
	c := &ClauseMergeInto{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseMergeInto should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseMergeUsing_Empty(t *testing.T) {
	c := &ClauseMergeUsing{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseMergeUsing should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseMergeOn_Empty(t *testing.T) {
	c := &ClauseMergeOn{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseMergeOn should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseWhenMatched_Empty(t *testing.T) {
	c := &ClauseWhenMatched{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseWhenMatched should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseWhenNotMatched_Empty(t *testing.T) {
	c := &ClauseWhenNotMatched{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	if out.Buff.Len() != 0 {
		t.Errorf("empty ClauseWhenNotMatched should produce no output, got %q", out.Buff.String())
	}
}

func TestClauseReplaceInto_Panic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic for nil table")
		}
	}()
	c := &ClauseReplaceInto{}
	out := &SQLBuilder{}
	c.Serialize(InsertStatementType, out)
}
