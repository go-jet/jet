package template

import "regexp"

// Returns the provided string as golang comment without ascii control characters
func formatGolangComment(comment string) string {
	if len(comment) == 0 {
		return ""
	}

	// Format as colang comment and remove ascii control characters from string
	return "// " + regexp.MustCompile(`[[:cntrl:]]+`).ReplaceAllString(comment, "")
}
