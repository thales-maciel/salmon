package salmon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Opts struct {
	TableName string // Name of the table to store applied migrations
	Dir string // Directory containing migration files
	FS fs.FS // Filesystem for reading migration files
}

func defaultOpts() *Opts {
	return &Opts{
		TableName: "salmon_schema_history",
		FS: osFS{},
		Dir: "migrations",
	}
}

type Migrations map[int64]Migration

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

	appliedMigrations, err := getAppliedMigrations(db, opts.TableName)
	if err != nil {
		return err
	}

	if _, err := fs.Stat(opts.FS, opts.Dir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("directory does not exist: %s", opts.Dir)
		}
		return err
	}

	files, err := fs.Glob(opts.FS, path.Join(opts.Dir, "*.sql"))
	if err != nil {
		return err
	}

	var migrationsToApply []Migration
	var versions []int64
	for _, file := range files {
		version, description, err := parseMigrationFile(file)
		if err != nil {
			return err
		}
		versions = append(versions, version)

		f, err := opts.FS.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		content, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		checksum := calculateChecksum(content)

		if int(version) < len(appliedMigrations) {
			migration := appliedMigrations[version]
			if migration.Checksum != checksum {
				err := fmt.Errorf("checksum does not match expected value: %s", file)
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

	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	for i, version := range versions {
		if i != int(version) {
			return fmt.Errorf("invalid version: expected %d, got %d", i, version)
		}
	}

	if len(migrationsToApply) == 0 {
		return nil
	}

	sort.Slice(migrationsToApply, func(i, j int) bool {
		return migrationsToApply[i].Version < migrationsToApply[j].Version
	})

	for _, migration := range migrationsToApply {
		if err := applyMigration(ctx, db, migration, opts.TableName); err != nil {
			return err
		}
	}

	return nil
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
		migration.Version, migration.Description, migration.Checksum,
	); err != nil {
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
		return 0, "", fmt.Errorf("invalid filename format: %s", basename)
	}

	version, err := strconv.Atoi(parts[0][1:]) // skip leading "V"
	if err != nil {
		return 0, "", fmt.Errorf("invalid filename format: %s", basename)
	}

	description := parts[1]
	return int64(version), description, nil
}

func getAppliedMigrations(db *sql.DB, tableName string) (Migrations, error) {
	migrations := make(Migrations)

	rows, err := db.Query(fmt.Sprintf("select version, description, checksum FROM %s where version > -1 order by version", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var migration Migration
		if err := rows.Scan(&migration.Version, &migration.Description, &migration.Checksum); err != nil {
			return nil, err
		}
		migrations[migration.Version] = migration
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

// osFS wraps functions working with os filesystem to implement fs.FS interfaces.
type osFS struct{}

func (osFS) Open(name string) (fs.File, error) { return os.Open(filepath.FromSlash(name)) }

func (osFS) ReadDir(name string) ([]fs.DirEntry, error) { return os.ReadDir(filepath.FromSlash(name)) }

func (osFS) Stat(name string) (fs.FileInfo, error) { return os.Stat(filepath.FromSlash(name)) }

func (osFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(filepath.FromSlash(name)) }

func (osFS) Glob(pattern string) ([]string, error) { return filepath.Glob(filepath.FromSlash(pattern)) }
