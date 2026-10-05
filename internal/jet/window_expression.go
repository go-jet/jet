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

func newWindowExpression(exp Expression) windowExpression {
	newExp := &windowExpressionImpl{}

	newExp.ExpressionInterfaceImpl.Root = newExp
	newExp.commonWindowImpl.expression = exp

	return newExp
}

type windowExpressionImpl struct {
	ExpressionInterfaceImpl
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
	newExp := &floatWindowExpressionImpl{}

	newExp.floatInterfaceImpl.root = newExp
	newExp.ExpressionInterfaceImpl.Root = newExp
	newExp.commonWindowImpl.expression = floatExp

	return newExp
}

type floatWindowExpressionImpl struct {
	floatInterfaceImpl
	ExpressionInterfaceImpl
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
	newExp := &integerWindowExpressionImpl{}

	newExp.integerInterfaceImpl.root = newExp
	newExp.ExpressionInterfaceImpl.Root = newExp
	newExp.commonWindowImpl.expression = intExp

	return newExp
}

type integerWindowExpressionImpl struct {
	integerInterfaceImpl
	ExpressionInterfaceImpl
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
	newExp := &boolWindowExpressionImpl{}

	newExp.boolInterfaceImpl.root = newExp
	newExp.ExpressionInterfaceImpl.Root = newExp
	newExp.commonWindowImpl.expression = boolExp

	return newExp
}

type boolWindowExpressionImpl struct {
	boolInterfaceImpl
	ExpressionInterfaceImpl
	commonWindowImpl
}

func (f *boolWindowExpressionImpl) OVER(window ...Window) BoolExpression {
	f.commonWindowImpl.over(window...)
	return f
}

func (f *boolWindowExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	f.commonWindowImpl.serialize(statement, out, FallTrough(options)...)
}
