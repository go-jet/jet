package utils

type Config struct {
	Exclude []string
}

func Contains(slist []string, str string) bool {
	for _, s := range slist {
		if s == str {
			return true
		}
	}

	return false
}
