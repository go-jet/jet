package jet

// RowLock is interface for SELECT statement row lock types
type RowLock interface {
	Serializer

	NOWAIT() RowLock
	SKIP_LOCKED() RowLock
}

type selectLockImpl struct {
	lockStrength       string
	noWait, skipLocked bool
}

// NewRowLock creates new RowLock
func NewRowLock(name string) func() RowLock {
	return func() RowLock {
		return newSelectLock(name)
	}
}

func newSelectLock(lockStrength string) RowLock {
	return &selectLockImpl{lockStrength: lockStrength}
}

func (s *selectLockImpl) NOWAIT() RowLock {
	s.noWait = true
	return s
}

func (s *selectLockImpl) SKIP_LOCKED() RowLock {
	s.skipLocked = true
	return s
}

func (s *selectLockImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(s.lockStrength)

	if s.noWait {
		out.WriteString("NOWAIT")
	}

	if s.skipLocked {
		out.WriteString("SKIP LOCKED")
	}
}
