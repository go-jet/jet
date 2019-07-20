package utils

import (
	"bytes"
	"github.com/go-jet/jet/internal/3rdparty/snaker"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// ToGoIdentifier converts database to Go identifier.
func ToGoIdentifier(databaseIdentifier string) string {
	return snaker.SnakeToCamel(replaceInvalidChars(databaseIdentifier))
}

// ToGoFileName converts database identifier to Go file name.
func ToGoFileName(databaseIdentifier string) string {
	return strings.ToLower(replaceInvalidChars(databaseIdentifier))
}

// SaveGoFile saves go file at folder dir, with name fileName and contents text.
func SaveGoFile(dirPath, fileName string, text []byte) error {
	newGoFilePath := filepath.Join(dirPath, fileName) + ".go"

	file, err := os.Create(newGoFilePath)

	if err != nil {
		return err
	}

	defer file.Close()

	p, err := format.Source(text)
	if err != nil {
		return err
	}

	_, err = file.Write(p)

	if err != nil {
		return err
	}

	return nil
}

// EnsureDirPath ensures dir path exists. If path does not exist, creates new path.
func EnsureDirPath(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, os.ModePerm)

		if err != nil {
			return err
		}
	}

	return nil
}

// GenerateTemplate generates template with template text and template data.
func GenerateTemplate(templateText string, templateData interface{}) ([]byte, error) {

	t, err := template.New("sqlBuilderTableTemplate").Funcs(template.FuncMap{
		"ToGoIdentifier": ToGoIdentifier,
		"now": func() string {
			return time.Now().Format(time.RFC850)
		},
	}).Parse(templateText)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, templateData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// CleanUpGeneratedFiles deletes everything at folder dir.
func CleanUpGeneratedFiles(dir string) error {
	exist, err := DirExists(dir)

	if err != nil {
		return err
	}

	if exist {
		err := os.RemoveAll(dir)

		if err != nil {
			return err
		}
	}

	return nil
}

// DirExists checks if folder at path exist.
func DirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func replaceInvalidChars(str string) string {
	str = strings.Replace(str, " ", "_", -1)
	str = strings.Replace(str, "-", "_", -1)

	return str
}

// FormatTimestamp formats t into Postgres' text format for timestamps. From: github.com/lib/pq
func FormatTimestamp(t time.Time) []byte {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}
	b := []byte(t.Format("2006-01-02 15:04:05.999999999Z07:00"))

	_, offset := t.Zone()
	offset = offset % 60
	if offset != 0 {
		// RFC3339Nano already printed the minus sign
		if offset < 0 {
			offset = -offset
		}

		b = append(b, ':')
		if offset < 10 {
			b = append(b, '0')
		}
		b = strconv.AppendInt(b, int64(offset), 10)
	}

	if bc {
		b = append(b, " BC"...)
	}
	return b
}

func IsNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
