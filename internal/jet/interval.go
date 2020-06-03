package jet

// Interval is internal common representation of sql interval
type Interval interface {
	Serializer
	IsInterval
}

// IsInterval interface
type IsInterval interface {
	isInterval()
}

// IsIntervalImpl is implementation of IsInterval interface
type IsIntervalImpl struct{}

func (i *IsIntervalImpl) isInterval() {}

// NewInterval creates new interval from serializer
func NewInterval(s Serializer) *IntervalImpl {
	newInterval := &IntervalImpl{
		interval: s,
	}

	return newInterval
}

// IntervalImpl is implementation of Interval type
type IntervalImpl struct {
	interval Serializer
	IsIntervalImpl
}

func (i IntervalImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("INTERVAL")
	i.interval.serialize(statement, out, FallTrough(options)...)
}
