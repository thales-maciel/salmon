package salmon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Opts struct {
	TableName string // Name of the table to store applied migrations
	Verbose bool // Enable verbose logging
}

func defaultOpts() *Opts {
	return &Opts{
		TableName: "salmon_schema_history",
	}
}

type Migration struct {
	Version     int64
	Description string
	Checksum    string
	Content     string
}

func Migrate(ctx context.Context, db *sql.DB, migrationDir string, opts *Opts) error {
	if opts == nil {
		opts = defaultOpts()
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, schema(opts.TableName)); err != nil {
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

	appliedMigrations, err := getAppliedMigrations(db, opts.TableName)
	if err != nil {
		return err
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
			err := fmt.Errorf("incorrect version number: %s", file.Name())
			return err
		}

		content, err := os.ReadFile(filepath.Join(migrationDir, file.Name()))
		if err != nil {
			return err
		}
		checksum := calculateChecksum(content)

		if i < len(appliedMigrations) {
			migration := appliedMigrations[i]
			if migration.Checksum != checksum {
				err := fmt.Errorf("checksum does not match expected value: %s", file.Name())
				return err
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
		if err := applyMigration(ctx, db, migration, opts.TableName); err != nil {
			return err
		}
	}

	return releaseLock(ctx, db, opts.TableName, nil)
}

func releaseLock(ctx context.Context, db *sql.DB, tableName string, err error) error {
	_, lockErr := db.ExecContext(ctx, fmt.Sprintf(`delete from %s where version = -1;`, tableName))
	if lockErr == nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("ATTENTION: could not release lock! please run `delete from %s where version=-1;` and try again.\noriginal err: %s\nfrom: %s", tableName, lockErr, err)
	}
	return fmt.Errorf("ATTENTION: could not release lock! please run `delete from %s where version=-1;` and try again.\noriginal err: %s", tableName, lockErr)
}

func applyMigration(ctx context.Context, db *sql.DB, migration Migration, tablename string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	var exists bool
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`select exists(select 1 from %s where version = $1)`, tablename), migration.Version).Scan(&exists)
	if err != nil {
		tx.Rollback()
		return err
	}

	if exists {
		return nil
	}

	if _, err = tx.ExecContext(ctx, `
        insert into salmon_schema_history (version, description, checksum)
        values ($1, $2, $3)`,
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

func getAppliedMigrations(db *sql.DB, tableName string) ([]Migration, error) {
	migrations := []Migration{}

	rows, err := db.Query(fmt.Sprintf("SELECT version, description, checksum FROM %s where version > -1 order by version", tableName))
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

func schema(tableName string) string {
	return fmt.Sprintf(`
		create table if not exists %s (
		id integer primary key autoincrement,
		version integer not null,
		description text not null,
		checksum text not null,
		applied_at timestamp default current_timestamp not null
		);
		`, tableName)
}

var (
	matchSQLComments = regexp.MustCompile(`(?m)^--.*$[\r\n]*`)
	matchEmptyEOL    = regexp.MustCompile(`(?m)^$[\r\n]*`) // TODO: Duplicate
)

func clearStatement(s string) string {
	s = matchSQLComments.ReplaceAllString(s, ``)
	return matchEmptyEOL.ReplaceAllString(s, ``)
}
