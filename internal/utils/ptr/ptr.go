package ptr

// Of returns the address of any given parameter
func Of[T any](value T) *T {
	return &value
}
