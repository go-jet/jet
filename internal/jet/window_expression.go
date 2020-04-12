package jet

type commonWindowImpl struct {
	expression Expression
	window     Window
}

func (w *commonWindowImpl) over(window ...Window) {
	if len(window) > 0 {
		w.window = window[0]
	} else {
		w.window = newWindowImpl(nil)
	}
}

func (w *commonWindowImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	w.expression.serialize(statement, out)
	if w.window != nil {
		out.WriteString("OVER")
		w.window.serialize(statement, out, FallTrough(options)...)
	}
}

// --------------------------------------

type windowExpression interface {
	Expression
	OVER(window ...Window) Expression
}

func newWindowExpression(Exp Expression) windowExpression {
	newExp := &windowExpressionImpl{
		Expression: Exp,
	}

	newExp.commonWindowImpl.expression = Exp

	return newExp
}

type windowExpressionImpl struct {
	Expression
	commonWindowImpl
}

func (f *windowExpressionImpl) OVER(window ...Window) Expression {
	f.commonWindowImpl.over(window...)
	return f
}

func (f *windowExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	f.commonWindowImpl.serialize(statement, out, FallTrough(options)...)
}

// -----------------------------------------------------

type floatWindowExpression interface {
	FloatExpression
	OVER(window ...Window) FloatExpression
}

func newFloatWindowExpression(floatExp FloatExpression) floatWindowExpression {
	newExp := &floatWindowExpressionImpl{
		FloatExpression: floatExp,
	}

	newExp.commonWindowImpl.expression = floatExp

	return newExp
}

type floatWindowExpressionImpl struct {
	FloatExpression
	commonWindowImpl
}

func (f *floatWindowExpressionImpl) OVER(window ...Window) FloatExpression {
	f.commonWindowImpl.over(window...)
	return f
}

func (f *floatWindowExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	f.commonWindowImpl.serialize(statement, out, FallTrough(options)...)
}

// ------------------------------------------------

type integerWindowExpression interface {
	IntegerExpression
	OVER(window ...Window) IntegerExpression
}

func newIntegerWindowExpression(intExp IntegerExpression) integerWindowExpression {
	newExp := &integerWindowExpressionImpl{
		IntegerExpression: intExp,
	}

	newExp.commonWindowImpl.expression = intExp

	return newExp
}

type integerWindowExpressionImpl struct {
	IntegerExpression
	commonWindowImpl
}

func (f *integerWindowExpressionImpl) OVER(window ...Window) IntegerExpression {
	f.commonWindowImpl.over(window...)
	return f
}

func (f *integerWindowExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	f.commonWindowImpl.serialize(statement, out, FallTrough(options)...)
}

// ------------------------------------------------

type boolWindowExpression interface {
	BoolExpression
	OVER(window ...Window) BoolExpression
}

func newBoolWindowExpression(boolExp BoolExpression) boolWindowExpression {
	newExp := &boolWindowExpressionImpl{
		BoolExpression: boolExp,
	}

	newExp.commonWindowImpl.expression = boolExp

	return newExp
}

type boolWindowExpressionImpl struct {
	BoolExpression
	commonWindowImpl
}

func (f *boolWindowExpressionImpl) OVER(window ...Window) BoolExpression {
	f.commonWindowImpl.over(window...)
	return f
}

func (f *boolWindowExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	f.commonWindowImpl.serialize(statement, out, FallTrough(options)...)
}
