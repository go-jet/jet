package mysql

import (
	"fmt"
	"github.com/go-jet/jet/v2/internal/jet"
)

// OptimizerHint provides a way to optimize query execution per-statement basis
type OptimizerHint = jet.OptimizerHint

// MAX_EXECUTION_TIME limits statement execution time
func MAX_EXECUTION_TIME(miliseconds int) OptimizerHint {
	return OptimizerHint(fmt.Sprintf("MAX_EXECUTION_TIME(%d)", miliseconds))
}

// QB_NAME assigns name to query block
func QB_NAME(name string) OptimizerHint {
	return OptimizerHint(fmt.Sprintf("QB_NAME(%s)", name))
}
