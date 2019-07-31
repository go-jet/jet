package common

type EqualityExpResult struct {
	Eq1          *bool
	Eq2          *bool
	Distinct1    *bool
	Distinct2    *bool
	NotDistinct1 *bool
	NotDistinct2 *bool
	Lt1          *bool
	Lt2          *bool
	Lte1         *bool
	Lte2         *bool
	Gt1          *bool
	Gt2          *bool
	Gte1         *bool
	Gte2         *bool
}

type AllTypesIntegerExpResult struct {
	EqualityExpResult `alias:"."`

	Add1           *int64
	Add2           *int64
	Sub1           *int64
	Sub2           *int64
	Mul1           *int64
	Mul2           *int64
	Div1           *int64
	Div2           *int64
	Mod1           *int64
	Mod2           *int64
	Pow1           *int64
	Pow2           *int64
	BitAnd1        *int64
	BitAnd2        *int64
	BitOr1         *int64
	BitOr2         *int64
	BitXor1        *int64
	BitXor2        *int64
	BitShiftLeft1  *int64
	BitShiftLeft2  *int64
	BitShiftRight1 *int64
	BitShiftRight2 *int64
}

type FloatExpressionTestResult struct {
	Eq1          *bool
	Eq2          *bool
	Eq3          *bool
	Distinct1    *bool
	Distinct2    *bool
	Distinct3    *bool
	NotDistinct1 *bool
	NotDistinct2 *bool
	NotDistinct3 *bool
	Lt1          *bool
	Lt2          *bool
	Gt1          *bool
	Gt2          *bool
	Add1         *float64
	Add2         *float64
	Sub1         *float64
	Sub2         *float64
	Mul1         *float64
	Mul2         *float64
	Div1         *float64
	Div2         *float64
	Mod1         *float64
	Mod2         *float64
	Pow1         *float64
	Pow2         *float64

	Abs    *float64
	Power  *float64
	Sqrt   *float64
	Cbrt   *float64
	Ceil   *float64
	Floor  *float64
	Round1 *float64
	Round2 *float64
	Sign   *float64
	Trunc  *float64
}
