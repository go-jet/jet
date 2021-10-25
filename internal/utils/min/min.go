package min

// Int returns minimum of two int values
func Int(a, b int) int {
	if a < b {
		return a
	}
	return b
}
