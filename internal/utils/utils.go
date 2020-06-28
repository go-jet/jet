package utils

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unicode"
)

// ToGoIdentifier converts database to Go identifier.
func ToGoIdentifier(databaseIdentifier string) string {
	return snaker.SnakeToCamel(replaceInvalidChars(databaseIdentifier))
}

// ToGoEnumValueIdentifier converts enum value name to Go identifier name.
func ToGoEnumValueIdentifier(enumName, enumValue string) string {
	enumValueIdentifier := ToGoIdentifier(enumValue)
	if !unicode.IsLetter([]rune(enumValueIdentifier)[0]) {
		return ToGoIdentifier(enumName) + enumValueIdentifier
	}

	return enumValueIdentifier
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

// DBClose closes non nil db connection
func DBClose(db *sql.DB) {
	if db == nil {
		return
	}

	db.Close()
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
	str = strings.Replace(str, ".", "_", -1)

	return str
}

// IsNil check if v is nil
func IsNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}

// MustBeTrue panics when condition is false
func MustBeTrue(condition bool, errorStr string) {
	if !condition {
		panic(errorStr)
	}
}

// MustBe panics with errorStr error, if v interface is not of reflect kind
func MustBe(v interface{}, kind reflect.Kind, errorStr string) {
	if reflect.TypeOf(v).Kind() != kind {
		panic(errorStr)
	}
}

// ValueMustBe panics with errorStr error, if v value is not of reflect kind
func ValueMustBe(v reflect.Value, kind reflect.Kind, errorStr string) {
	if v.Kind() != kind {
		panic(errorStr)
	}
}

// TypeMustBe panics with errorStr error, if v type is not of reflect kind
func TypeMustBe(v reflect.Type, kind reflect.Kind, errorStr string) {
	if v.Kind() != kind {
		panic(errorStr)
	}
}

// MustBeInitializedPtr panics with errorStr if val interface is nil
func MustBeInitializedPtr(val interface{}, errorStr string) {
	if IsNil(val) {
		panic(errorStr)
	}
}

// PanicOnError panics if err is not nil
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// ErrorCatch is used in defer to recover from panics and to set err
func ErrorCatch(err *error) {
	recovered := recover()

	if recovered == nil {
		return
	}

	recoveredErr, isError := recovered.(error)

	if isError {
		*err = recoveredErr
	} else {
		*err = fmt.Errorf("%v", recovered)
	}
}

// StringSliceContains checks if slice of strings contains a string
func StringSliceContains(strings []string, contains string) bool {
	for _, str := range strings {
		if str == contains {
			return true
		}
	}

	return false
}

// ExtractDateTimeComponents extracts number of days, hours, minutes, seconds, microseconds from duration
func ExtractDateTimeComponents(duration time.Duration) (days, hours, minutes, seconds, microseconds int64) {
	days = int64(duration / (24 * time.Hour))
	reminder := duration % (24 * time.Hour)

	hours = int64(reminder / time.Hour)
	reminder = reminder % time.Hour

	minutes = int64(reminder / time.Minute)
	reminder = reminder % time.Minute

	seconds = int64(reminder / time.Second)
	reminder = reminder % time.Second

	microseconds = int64(reminder / time.Microsecond)

	return
}
