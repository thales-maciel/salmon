package salmon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MigrationFiles map[string]string

var (
	outOfOrder = MigrationFiles{
		"V0__initial_schema.sql":   "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql": "alter table users add email text not null;",
		"V3__add_age_column.sql":   "alter table users add age integer not null;",
	}
	invalidName = MigrationFiles{
		"V0__initial_schema.sql":     "create table users (id integer primary key, name text not null);",
		"Vone__add_email_column.sql": "alter table users add email text not null;",
	}
	invalidNameMissingVersion = MigrationFiles{
		"V0__initial_schema.sql": "create table users (id integer primary key, name text not null);",
		"__add_email_column.sql": "alter table users add email text not null;",
	}
	invalidNameEmptyDescription = MigrationFiles{
		"V0__initial_schema.sql": "create table users (id integer primary key, name text not null);",
		"V1__.sql":               "alter table users add email text not null;",
	}
	invalidSql = MigrationFiles{
		"V0__initial_schema.sql":   "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql": "alter table users add",
	}
	validMigrations = MigrationFiles{
		"V0__initial_schema.sql":   "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql": "alter table users add email text not null;",
		"V2__add_age_column.sql":   "alter table users add age integer not null;",
	}
	validLong = MigrationFiles{
		"V0__initial_schema.sql":          "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql":        "alter table users add email text not null;",
		"V2__add_age_column.sql":          "alter table users add age integer not null;",
		"V3__add_description_column.sql":  "create table if not exists user (id);",
		"V4__add_description_column.sql":  "create table if not exists user (id);",
		"V5__add_description_column.sql":  "create table if not exists user (id);",
		"V6__add_description_column.sql":  "create table if not exists user (id);",
		"V7__add_description_column.sql":  "create table if not exists user (id);",
		"V8__add_description_column.sql":  "create table if not exists user (id);",
		"V9__add_description_column.sql":  "create table if not exists user (id);",
		"V10__add_description_column.sql": "create table if not exists user (id);",
	}
)

func setupDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	require.NotNil(t, db)

	return db
}

func setupMigrationsDir(t *testing.T, files MigrationFiles) string {
	dir, err := os.MkdirTemp("", "salmon_test")
	require.NoError(t, err)

	for filename, content := range files {
		filePath := filepath.Join(dir, filename)
		err := os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)
	}

	return dir
}

func seedAppliedMigration(t *testing.T, db *sql.DB, tableName string, filename string, content string) {
	version, description, err := parseMigrationFile(filename)
	require.NoError(t, err)

	_, err = db.Exec(
		fmt.Sprintf("insert into %s (version, description, checksum) values (?, ?, ?)", tableName),
		version,
		description,
		calculateChecksum([]byte(content)),
	)
	require.NoError(t, err)
}

func mustBuildMigration(t *testing.T, filename string, content string) Migration {
	version, description, err := parseMigrationFile(filename)
	require.NoError(t, err)

	return Migration{
		Version:     version,
		Description: description,
		Checksum:    calculateChecksum([]byte(content)),
		Content:     content,
	}
}

func TestMigrate(t *testing.T) {
	tests := []struct {
		name             string
		files            MigrationFiles
		expectedError    string
		expectedVersions []int
	}{
		{
			name:             "out of order migrations",
			files:            outOfOrder,
			expectedError:    "invalid version: expected 2, got 3",
			expectedVersions: nil,
		},
		{
			name:             "invalid migration name",
			files:            invalidName,
			expectedError:    "invalid filename format: Vone__add_email_column.sql",
			expectedVersions: nil,
		},
		{
			name:             "invalid migration name missing version",
			files:            invalidNameMissingVersion,
			expectedError:    "invalid filename format: __add_email_column.sql",
			expectedVersions: nil,
		},
		{
			name:             "invalid migration name empty description",
			files:            invalidNameEmptyDescription,
			expectedError:    "invalid filename format: V1__.sql",
			expectedVersions: nil,
		},
		{
			name:             "invalid SQL in migration",
			files:            invalidSql,
			expectedError:    "incomplete input",
			expectedVersions: nil,
		},
		{
			name:             "valid migrations",
			files:            validMigrations,
			expectedError:    "",
			expectedVersions: []int{0, 1, 2},
		},
		{
			name:             "valid with names not naturally ordered",
			files:            validLong,
			expectedError:    "",
			expectedVersions: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := setupDB(t)
			defer db.Close()

			dir := setupMigrationsDir(t, tt.files)
			defer os.RemoveAll(dir)

			opts := defaultOpts()
			opts.Dir = dir
			err := Migrate(ctx, db, opts)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, tt.expectedError, err.Error())
			} else {
				assert.NoError(t, err, fmt.Sprintf("expected no error, got: %v", err))
			}

			if tt.expectedVersions != nil {
				rows, err := db.Query(fmt.Sprintf("select version from %s order by version", opts.TableName))
				require.NoError(t, err)
				defer rows.Close()

				var versions []int
				for rows.Next() {
					var version int
					err := rows.Scan(&version)
					require.NoError(t, err)
					versions = append(versions, version)
				}

				assert.Equal(t, tt.expectedVersions, versions)
			}
		})
	}

}

func TestMigrateWithPartialOptsUsesDefaults(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	defer db.Close()

	dir := setupMigrationsDir(t, validMigrations)
	defer os.RemoveAll(dir)

	err := Migrate(ctx, db, &Opts{
		Dir: dir,
	})
	require.NoError(t, err)

	rows, err := db.Query("select version from salmon_schema_history order by version")
	require.NoError(t, err)
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		err := rows.Scan(&version)
		require.NoError(t, err)
		versions = append(versions, version)
	}

	assert.Equal(t, []int{0, 1, 2}, versions)
}

func TestMigrateRejectsNonContiguousAppliedHistory(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	defer db.Close()

	opts := defaultOpts()
	_, err := db.Exec(schema(opts.TableName))
	require.NoError(t, err)

	seedAppliedMigration(t, db, opts.TableName, "V0__initial_schema.sql", validMigrations["V0__initial_schema.sql"])
	seedAppliedMigration(t, db, opts.TableName, "V2__add_age_column.sql", validMigrations["V2__add_age_column.sql"])

	dir := setupMigrationsDir(t, validMigrations)
	defer os.RemoveAll(dir)
	opts.Dir = dir

	err = Migrate(ctx, db, opts)
	require.Error(t, err)
	assert.Equal(t, "failed to retrieve applied migrations: invalid applied migration history: expected version 1, got 2", err.Error())
}

func TestSchemaEnforcesUniqueMigrationVersion(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	opts := defaultOpts()
	_, err := db.Exec(schema(opts.TableName))
	require.NoError(t, err)

	seedAppliedMigration(t, db, opts.TableName, "V0__initial_schema.sql", validMigrations["V0__initial_schema.sql"])

	_, err = db.Exec(
		fmt.Sprintf("insert into %s (version, description, checksum) values (?, ?, ?)", quoteIdentifier(opts.TableName)),
		0,
		"duplicate",
		"duplicate",
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "UNIQUE constraint failed")
}

func TestApplyMigrationDoesNotLeakTransactionWhenAlreadyApplied(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	defer db.Close()
	db.SetMaxOpenConns(1)

	opts := defaultOpts()
	_, err := db.Exec(schema(opts.TableName))
	require.NoError(t, err)

	migration := mustBuildMigration(t, "V0__initial_schema.sql", validMigrations["V0__initial_schema.sql"])

	err = applyMigration(ctx, db, migration, opts.TableName)
	require.NoError(t, err)

	err = applyMigration(ctx, db, migration, opts.TableName)
	require.NoError(t, err)

	queryCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	var count int
	err = db.QueryRowContext(queryCtx, "select count(*) from salmon_schema_history").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
