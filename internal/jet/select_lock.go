package jet

// RowLock is interface for SELECT statement row lock types
type RowLock interface {
	Serializer

	OF(...Table) RowLock
	NOWAIT() RowLock
	SKIP_LOCKED() RowLock
}

type selectLockImpl struct {
	lockStrength       string
	of                 []Table
	noWait, skipLocked bool
}

// NewRowLock creates new RowLock
func NewRowLock(name string) func() RowLock {
	return func() RowLock {
		return newSelectLock(name)
	}
}

func newSelectLock(lockStrength string) *selectLockImpl {
	return &selectLockImpl{lockStrength: lockStrength}
}

func (s *selectLockImpl) OF(tables ...Table) RowLock {
	s.of = tables
	return s
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

	if len(s.of) > 0 {
		out.WriteString("OF")

		for i, of := range s.of {
			if i > 0 {
				out.WriteString(", ")
			}

			table := of.Alias()
			if table == "" {
				table = of.TableName()
			}

			out.WriteIdentifier(table)
		}
	}

	if s.noWait {
		out.WriteString("NOWAIT")
	}

	if s.skipLocked {
		out.WriteString("SKIP LOCKED")
	}
}
