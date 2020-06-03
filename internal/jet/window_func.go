package jet

// Window interface
type Window interface {
	Serializer
	ORDER_BY(expr ...OrderByClause) Window
	ROWS(start FrameExtent, end ...FrameExtent) Window
	RANGE(start FrameExtent, end ...FrameExtent) Window
	GROUPS(start FrameExtent, end ...FrameExtent) Window
}

type windowImpl struct {
	partitionBy []Expression
	orderBy     ClauseOrderBy
	frameUnits  string
	start, end  FrameExtent

	parent Window
}

func newWindowImpl(parent Window) *windowImpl {
	newWindow := &windowImpl{}
	if parent == nil {
		newWindow.parent = newWindow
	} else {
		newWindow.parent = parent
	}

	return newWindow
}

func (w *windowImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if !contains(options, NoWrap) {
		out.WriteByte('(')
	}

	if w.partitionBy != nil {
		out.WriteString("PARTITION BY")

		serializeExpressionList(statement, w.partitionBy, ", ", out)
	}
	w.orderBy.SkipNewLine = true
	w.orderBy.Serialize(statement, out, FallTrough(options)...)

	if w.frameUnits != "" {
		out.WriteString(w.frameUnits)

		if w.end == nil {
			w.start.serialize(statement, out)
		} else {
			out.WriteString("BETWEEN")
			w.start.serialize(statement, out)
			out.WriteString("AND")
			w.end.serialize(statement, out)
		}
	}

	if !contains(options, NoWrap) {
		out.WriteByte(')')
	}
}

func (w *windowImpl) ORDER_BY(exprs ...OrderByClause) Window {
	w.orderBy.List = exprs
	return w.parent
}

func (w *windowImpl) ROWS(start FrameExtent, end ...FrameExtent) Window {
	w.frameUnits = "ROWS"
	w.setFrameRange(start, end...)
	return w.parent
}

func (w *windowImpl) RANGE(start FrameExtent, end ...FrameExtent) Window {
	w.frameUnits = "RANGE"
	w.setFrameRange(start, end...)
	return w.parent
}

func (w *windowImpl) GROUPS(start FrameExtent, end ...FrameExtent) Window {
	w.frameUnits = "GROUPS"
	w.setFrameRange(start, end...)
	return w.parent
}

func (w *windowImpl) setFrameRange(start FrameExtent, end ...FrameExtent) {
	w.start = start
	if len(end) > 0 {
		w.end = end[0]
	}
}

// PARTITION_BY window function constructor
func PARTITION_BY(exp Expression, exprs ...Expression) Window {
	funImpl := newWindowImpl(nil)
	funImpl.partitionBy = append([]Expression{exp}, exprs...)
	return funImpl
}

// ORDER_BY window function constructor
func ORDER_BY(expr ...OrderByClause) Window {
	funImpl := newWindowImpl(nil)
	funImpl.orderBy.List = expr
	return funImpl
}

// -----------------------------------------------

// FrameExtent interface
type FrameExtent interface {
	Serializer
	isFrameExtent()
}

// PRECEDING window frame clause
func PRECEDING(offset Serializer) FrameExtent {
	return &frameExtentImpl{
		preceding: true,
		offset:    offset,
	}
}

// FOLLOWING window frame clause
func FOLLOWING(offset Serializer) FrameExtent {
	return &frameExtentImpl{
		preceding: false,
		offset:    offset,
	}
}

type frameExtentImpl struct {
	preceding bool
	offset    Serializer
}

func (f *frameExtentImpl) isFrameExtent() {}

func (f *frameExtentImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if f == nil {
		return
	}
	f.offset.serialize(statement, out, FallTrough(options)...)

	if f.preceding {
		out.WriteString("PRECEDING")
	} else {
		out.WriteString("FOLLOWING")
	}
}

// -----------------------------------------------

// Window function keywords
var (
	UNBOUNDED   = Keyword("UNBOUNDED")
	CURRENT_ROW = frameExtentKeyword{"CURRENT ROW"}
)

type frameExtentKeyword struct {
	Keyword
}

func (f frameExtentKeyword) isFrameExtent() {}

// -----------------------------------------------

// WindowName is used to specify window reference from WINDOW clause
func WindowName(name string) Window {
	newWindow := &windowName{name: name}
	newWindow.parent = newWindow
	return newWindow
}

type windowName struct {
	windowImpl
	name string
}

func (w windowName) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteByte('(')

	out.WriteString(w.name)
	w.windowImpl.serialize(statement, out, NoWrap.WithFallTrough(options)...)

	out.WriteByte(')')
}
