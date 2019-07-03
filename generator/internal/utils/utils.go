package utils

import (
	"bytes"
	"github.com/go-jet/jet/internal/util"
	"go/format"
	"os"
	"path/filepath"
	"text/template"
	"time"
)

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

func EnsureDirPath(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, os.ModePerm)

		if err != nil {
			return err
		}
	}

	return nil
}

func GenerateTemplate(templateText string, templateData interface{}) ([]byte, error) {

	t, err := template.New("sqlBuilderTableTemplate").Funcs(template.FuncMap{
		"ToGoIdentifier": util.ToGoIdentifier,
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
