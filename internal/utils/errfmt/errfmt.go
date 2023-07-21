package errfmt

import (
	"strings"
)

// Trace returns well formatted wrapped error trace string
func Trace(err error) string {
	return "Error trace:\n" + " - " + strings.Replace(err.Error(), ": ", ":\n - ", -1)
}
