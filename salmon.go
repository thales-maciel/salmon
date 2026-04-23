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
	Dir       string // Directory containing migration files
	FS        fs.FS  // Filesystem for reading migration files
}

func defaultOpts() *Opts {
	return &Opts{
		TableName: "salmon_schema_history",
		FS:        osFS{},
		Dir:       "migrations",
	}
}

func normalizeOpts(opts *Opts) *Opts {
	defaults := defaultOpts()
	if opts == nil {
		return defaults
	}

	normalized := *opts
	if normalized.TableName == "" {
		normalized.TableName = defaults.TableName
	}
	if normalized.Dir == "" {
		normalized.Dir = defaults.Dir
	}
	if normalized.FS == nil {
		normalized.FS = defaults.FS
	}

	return &normalized
}

type Migrations map[int64]Migration

type Migration struct {
	Version     int64
	Description string
	Checksum    string
	Content     string
	Filename    string
}

func Migrate(ctx context.Context, db *sql.DB, opts *Opts) error {
	opts = normalizeOpts(opts)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, schema(opts.TableName)); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create schema table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	appliedMigrations, err := getAppliedMigrations(db, opts.TableName)
	if err != nil {
		return fmt.Errorf("failed to retrieve applied migrations: %w", err)
	}

	if _, err := fs.Stat(opts.FS, opts.Dir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("directory does not exist: %s", opts.Dir)
		}
		return fmt.Errorf("failed to read directory: %w", err)
	}

	files, err := fs.Glob(opts.FS, path.Join(opts.Dir, "*.sql"))
	if err != nil {
		return fmt.Errorf("failed to read migration files: %w", err)
	}

	var migrationsToApply []Migration
	var versions []int64
	for _, file := range files {
		version, description, err := parseMigrationFile(file)
		if err != nil {
			return err
		}
		versions = append(versions, version)

		content, err := getFileContent(opts.FS, file)
		if err != nil {
			return fmt.Errorf("failed to read migration file content: %s: %w", file, err)
		}
		checksum := calculateChecksum(content)

		if appliedMigration, ok := appliedMigrations[version]; ok {
			if appliedMigration.Checksum != checksum {
				return fmt.Errorf("checksum does not match expected value: %s", file)
			}
			continue
		}
		migrationsToApply = append(migrationsToApply, Migration{
			Version:     version,
			Description: description,
			Checksum:    checksum,
			Content:     string(content),
			Filename:    file,
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

func getFileContent(fs fs.FS, file string) ([]byte, error) {
	f, err := fs.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}

func applyMigration(ctx context.Context, db *sql.DB, migration Migration, tablename string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, fmt.Sprintf(`
        insert or ignore into %s (version, description, checksum)
        values ($1, $2, $3);`, quoteIdentifier(tablename)),
		migration.Version, migration.Description, migration.Checksum,
	)
	if err != nil {
		return fmt.Errorf("failed to insert migration into history table: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check inserted migration row count: %w", err)
	}

	if rowsAffected == 0 {
		return nil
	}

	if _, err = tx.ExecContext(ctx, migration.Content); err != nil {
		return fmt.Errorf(
			"failed to execute migration version %d (%s): %w",
			migration.Version,
			migrationFilename(migration),
			err,
		)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration transaction: %w", err)
	}

	return nil
}

func calculateChecksum(content []byte) string {
	checksum := sha256.Sum256(content)
	return hex.EncodeToString(checksum[:])
}

func parseMigrationFile(filename string) (int64, string, error) {
	basename := filepath.Base(filename)
	if !strings.HasPrefix(basename, "V") || !strings.HasSuffix(basename, ".sql") {
		return 0, "", fmt.Errorf("invalid filename format: %s", basename)
	}

	name := strings.TrimSuffix(basename, ".sql")
	parts := strings.SplitN(name, "__", 2) // split version and description
	if len(parts) != 2 || len(parts[0]) < 2 || parts[1] == "" {
		return 0, "", fmt.Errorf("invalid filename format: %s", basename)
	}

	version, err := strconv.ParseInt(parts[0][1:], 10, 64) // skip leading "V"
	if err != nil {
		return 0, "", fmt.Errorf("invalid filename format: %s", basename)
	}
	if version < 0 {
		return 0, "", fmt.Errorf("invalid filename format: %s", basename)
	}

	description := parts[1]
	return version, description, nil
}

func getAppliedMigrations(db *sql.DB, tableName string) (Migrations, error) {
	migrations := make(Migrations)

	rows, err := db.Query(fmt.Sprintf("select version, description, checksum FROM %s where version > -1 order by version", quoteIdentifier(tableName)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var migration Migration
		if err := rows.Scan(&migration.Version, &migration.Description, &migration.Checksum); err != nil {
			return nil, err
		}

		expectedVersion := int64(len(migrations))
		if migration.Version != expectedVersion {
			return nil, fmt.Errorf("invalid applied migration history: expected version %d, got %d", expectedVersion, migration.Version)
		}

		migrations[migration.Version] = migration
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return migrations, nil
}

func schema(tableName string) string {
	quotedTableName := quoteIdentifier(tableName)
	quotedVersionIndexName := quoteIdentifier(fmt.Sprintf("%s_version_idx", tableName))

	return fmt.Sprintf(`
		create table if not exists %s (
		id integer primary key autoincrement,
		version integer not null,
		description text not null,
		checksum text not null,
		applied_at timestamp default current_timestamp not null
		);
		create unique index if not exists %s on %s (version);
		`, quotedTableName, quotedVersionIndexName, quotedTableName)
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func migrationFilename(migration Migration) string {
	if migration.Filename == "" {
		return fmt.Sprintf("V%d", migration.Version)
	}

	return filepath.Base(migration.Filename)
}

// osFS wraps functions working with os filesystem to implement fs.FS interfaces.
type osFS struct{}

func (osFS) Open(name string) (fs.File, error) { return os.Open(filepath.FromSlash(name)) }

func (osFS) ReadDir(name string) ([]fs.DirEntry, error) { return os.ReadDir(filepath.FromSlash(name)) }

func (osFS) Stat(name string) (fs.FileInfo, error) { return os.Stat(filepath.FromSlash(name)) }

func (osFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(filepath.FromSlash(name)) }

func (osFS) Glob(pattern string) ([]string, error) { return filepath.Glob(filepath.FromSlash(pattern)) }
