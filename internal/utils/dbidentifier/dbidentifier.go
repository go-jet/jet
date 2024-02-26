package dbidentifier

import (
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"strings"
	"unicode"
)

// ToGoIdentifier converts database identifier to Go identifier.
func ToGoIdentifier(databaseIdentifier string) string {
	return snaker.SnakeToCamel(replaceInvalidChars(databaseIdentifier))
}

// ToGoFileName converts database identifier to Go file name.
func ToGoFileName(databaseIdentifier string) string {
	return strings.ToLower(replaceInvalidChars(databaseIdentifier))
}

func replaceInvalidChars(identifier string) string {
	increase, needs := needsCharReplacement(identifier)

	if !needs {
		return identifier
	}

	var b strings.Builder

	b.Grow(len(identifier) + increase)

	for _, c := range identifier {
		switch {
		case unicode.IsSpace(c):
			b.WriteByte('_')
		case unicode.IsControl(c):
			continue
		default:
			replacement, ok := asciiCharacterReplacement[c]

			if ok {
				b.WriteByte('_')
				b.WriteString(replacement)
				b.WriteByte('_')
			} else {
				b.WriteRune(c)
			}
		}

	}

	return b.String()
}

func needsCharReplacement(identifier string) (increase int, needs bool) {
	for _, c := range identifier {
		switch {
		case unicode.IsSpace(c):
			needs = true
		case unicode.IsControl(c):
			increase += -1
			needs = true
			continue
		default:
			replacement, ok := asciiCharacterReplacement[c]

			if ok {
				increase += len(replacement) + 1
				needs = true
			}
		}
	}

	return increase, needs
}

var asciiCharacterReplacement = map[rune]string{
	'!':  "exclamation",
	'"':  "quotation",
	'#':  "number",
	'$':  "dollar",
	'%':  "percent",
	'&':  "ampersand",
	'\'': "apostrophe",
	'(':  "opening_parentheses",
	')':  "closing_parentheses",
	'*':  "asterisk",
	'+':  "plus",
	',':  "comma",
	'-':  "_",
	'.':  "_",
	'/':  "slash",
	':':  "colon",
	';':  "semicolon",
	'<':  "less",
	'=':  "equal",
	'>':  "greater",
	'?':  "question",
	'@':  "at",
	'[':  "opening_bracket",
	'\\': "backslash",
	']':  "closing_bracket",
	'^':  "caret",
	'`':  "accent",
	'{':  "opening_braces",
	'|':  "vertical_bar",
	'}':  "closing_braces",
	'~':  "tilde",
}
