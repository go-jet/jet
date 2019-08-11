package jet

// SelectLock is interface for SELECT statement locks
type SelectLock interface {
	Serializer

	NOWAIT() SelectLock
	SKIP_LOCKED() SelectLock
}

type selectLockImpl struct {
	lockStrength       string
	noWait, skipLocked bool
}

func NewSelectLock(name string) func() SelectLock {
	return func() SelectLock {
		return newSelectLock(name)
	}
}

func newSelectLock(lockStrength string) SelectLock {
	return &selectLockImpl{lockStrength: lockStrength}
}

func (s *selectLockImpl) NOWAIT() SelectLock {
	s.noWait = true
	return s
}

func (s *selectLockImpl) SKIP_LOCKED() SelectLock {
	s.skipLocked = true
	return s
}

func (s *selectLockImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString(s.lockStrength)

	if s.noWait {
		out.WriteString("NOWAIT")
	}

	if s.skipLocked {
		out.WriteString("SKIP LOCKED")
	}

	return nil
}
