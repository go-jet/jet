package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
)

func ExecFile(db *sql.DB, sqlFilePath string) error {
	testSampleSql, err := os.ReadFile(sqlFilePath)
	if err != nil {
		return fmt.Errorf("failed to read sql file - %s: %w", sqlFilePath, err)
	}

	err = ExecInTx(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(string(testSampleSql))
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to execute sql file - %s: %w", sqlFilePath, err)
	}

	return nil
}

func ExecInTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted, // to speed up initialization of test database
	})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	err = f(tx)

	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction")
	}

	return nil
}
