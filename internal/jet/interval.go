package jet

type Interval interface {
	Serializer
	IsInterval
}

type IsInterval interface {
	isInterval()
}

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
