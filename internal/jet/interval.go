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

// NewInterval creates new interval from serializer
func NewInterval(s Serializer) Interval {
	newInterval := &intervalImpl{
		interval: s,
	}

	return newInterval
}

type intervalImpl struct {
	interval Serializer
}

func (i intervalImpl) isInterval() {}

func (i intervalImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("INTERVAL")
	i.interval.serialize(statement, out, options...)
}
