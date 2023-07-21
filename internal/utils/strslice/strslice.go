package strslice

// Contains checks if slice of strings contains a string
func Contains(strings []string, contains string) bool {
	for _, str := range strings {
		if str == contains {
			return true
		}
	}

	return false
}
