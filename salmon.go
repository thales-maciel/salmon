package salmon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Migration struct {
	Version     int64
	Description string
	Checksum    string
	Content     string
}

func Migrate(ctx context.Context, db *sql.DB, migrationDir string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, schema); err != nil {
		tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}


	files, err := os.ReadDir(migrationDir)
	if err != nil {
		return err
	}

	appliedMigrations, err := getAppliedMigrations(db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %v", err)
	}

	var migrationsToApply []Migration
	for i, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".sql" {
			continue
		}

		version, description, err := parseMigrationFile(file.Name())
		if err != nil {
			return err
		}

		if version != int64(i) {
			return fmt.Errorf("incorrect version number: %s", file.Name())
		}

		content, err := os.ReadFile(filepath.Join(migrationDir, file.Name()))
		if err != nil {
			return err
		}
		checksum := calculateChecksum(content)

		if i < len(appliedMigrations) {
			migration := appliedMigrations[i]
			if migration.Checksum != checksum {
				return fmt.Errorf("checksum does not match expected value: %s", file.Name())
			}
			continue
		}
		migrationsToApply = append(migrationsToApply, Migration{
			Version:     version,
			Description: description,
			Checksum:    checksum,
			Content:     string(content),
		})
	}

	if len(migrationsToApply) == 0 {
		return nil
	}

	for _, migration := range migrationsToApply {
		if err := applyMigration(ctx, db, migration); err != nil {
			return err
		}
	}

	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, migration Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, `
        INSERT INTO salmon_schema_history (version, description, checksum)
        VALUES ($1, $2, $3)`,
		migration.Version, migration.Description, migration.Checksum); err != nil {
		tx.Rollback()
		return err
	}

	if _, err = tx.ExecContext(ctx, migration.Content); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func calculateChecksum(content []byte) string {
	checksum := sha256.Sum256(content)
	return hex.EncodeToString(checksum[:])
}

func parseMigrationFile(filename string) (int64, string, error) {
	basename := filepath.Base(filename)

	parts := strings.SplitN(basename, "__", 2) // split version and description
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid filename format: %s", filename)
	}

	version, err := strconv.Atoi(parts[0][1:]) // skip leading "V"
	if err != nil {
		return 0, "", fmt.Errorf("invalid filename format: %s", filename)
	}

	description := parts[1]
	return int64(version), description, nil
}

func getAppliedMigrations(db *sql.DB) ([]Migration, error) {
	migrations := []Migration{}

	rows, err := db.Query("SELECT version, description, checksum FROM salmon_schema_history order by version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var migration Migration
		if err := rows.Scan(&migration.Version, &migration.Description, &migration.Checksum); err != nil {
			return nil, err
		}
		migrations = append(migrations, migration)
	}

	return migrations, nil
}

const schema = `
    create table if not exists salmon_schema_history (
        id integer primary key autoincrement,
        version integer not null,
        description text not null,
        checksum text not null,
        applied_at timestamp default current_timestamp not null
    );
    create table if not exists salmon_lock (
        id integer primary key,
        locked_at timestamp default current_timestamp not null
    );
    `
