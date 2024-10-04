package common

import "os"

const (
	GhSkipComments = "TEST_SKIP_COMMENTS"
)

// Add a hack to bypass failing tests
func IsHack() bool {
	return os.Getenv(GhSkipComments) == "1"
	//return true
}
