package sqlbuilder

import (
	"testing"
)

func TestFloatExpressionEQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.EQ(table2ColFloat), "(table1.colFloat = table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.EQ(Float(2.11)), "(table1.colFloat = $1)", float64(2.11))
}

func TestFloatExpressionNOT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.NOT_EQ(table2ColFloat), "(table1.colFloat != table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.NOT_EQ(Float(2.11)), "(table1.colFloat != $1)", float64(2.11))
}

func TestFloatExpressionGT(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.GT(table2ColFloat), "(table1.colFloat > table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.GT(Float(2.11)), "(table1.colFloat > $1)", float64(2.11))
}

func TestFloatExpressionGT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.GT_EQ(table2ColFloat), "(table1.colFloat >= table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.GT_EQ(Float(2.11)), "(table1.colFloat >= $1)", float64(2.11))
}

func TestFloatExpressionLT(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.LT(table2ColFloat), "(table1.colFloat < table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.LT(Float(2.11)), "(table1.colFloat < $1)", float64(2.11))
}

func TestFloatExpressionLT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.LT_EQ(table2ColFloat), "(table1.colFloat <= table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.LT_EQ(Float(2.11)), "(table1.colFloat <= $1)", float64(2.11))
}

func TestFloatExpressionADD(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.ADD(table2ColFloat), "(table1.colFloat + table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.ADD(Float(2.11)), "(table1.colFloat + $1)", float64(2.11))
}

func TestFloatExpressionSUB(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.SUB(table2ColFloat), "(table1.colFloat - table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.SUB(Float(2.11)), "(table1.colFloat - $1)", float64(2.11))
}

func TestFloatExpressionMUL(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.MUL(table2ColFloat), "(table1.colFloat * table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.MUL(Float(2.11)), "(table1.colFloat * $1)", float64(2.11))
}

func TestFloatExpressionDIV(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.DIV(table2ColFloat), "(table1.colFloat / table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.DIV(Float(2.11)), "(table1.colFloat / $1)", float64(2.11))
}

func TestFloatExpressionMOD(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.MOD(table2ColFloat), "(table1.colFloat % table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.MOD(Float(2.11)), "(table1.colFloat % $1)", float64(2.11))
}

func TestFloatExpressionPOW(t *testing.T) {
	assertExpressionSerialize(t, table1ColFloat.POW(table2ColFloat), "(table1.colFloat ^ table2.colFloat)")
	assertExpressionSerialize(t, table1ColFloat.POW(Float(2.11)), "(table1.colFloat ^ $1)", float64(2.11))
}
