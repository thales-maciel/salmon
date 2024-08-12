package salmon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MigrationFiles map[string]string

var (
	outOfOrder = MigrationFiles{
		"V0__initial_schema.sql": "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql": "alter table users add email text not null;",
		"V3__add_age_column.sql": "alter table users add age integer not null;",
	}
	invalidName = MigrationFiles{
		"V0__initial_schema.sql": "create table users (id integer primary key, name text not null);",
		"Vone__add_email_column.sql": "alter table users add email text not null;",
	}
	invalidSql = MigrationFiles{
		"V0__initial_schema.sql": "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql": "alter table users add",
	}
	validMigrations = MigrationFiles{
		"V0__initial_schema.sql": "create table users (id integer primary key, name text not null);",
		"V1__add_email_column.sql": "alter table users add email text not null;",
		"V2__add_age_column.sql": "alter table users add age integer not null;",
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

func TestMigrate(t *testing.T) {
	tests := []struct{
		name string
		files MigrationFiles
		expectedError string
		expectedVersions []int
	}{
		{
			name: "out of order migrations",
			files: outOfOrder,
			expectedError: "incorrect version number: V3__add_age_column.sql",
			expectedVersions: nil,
		},
		{
			name: "invalid migration name",
			files: invalidName,
			expectedError: "invalid filename format: Vone__add_email_column.sql",
			expectedVersions: nil,
		},
		{
			name: "invalid SQL in migration",
			files: invalidSql,
			expectedError: "incomplete input",
			expectedVersions: nil,
		},
		{
			name: "valid migrations",
			files: validMigrations,
			expectedError: "",
			expectedVersions: []int{0, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := setupDB(t)
			defer db.Close()

			dir := setupMigrationsDir(t, tt.files)
			defer os.RemoveAll(dir)

			err := Migrate(ctx, db, dir)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err.Error())
			} else {
				assert.NoError(t, err, fmt.Sprintf("expected no error, got: %v", err))
			}
			
			if tt.expectedVersions != nil {
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

				assert.Equal(t, tt.expectedVersions, versions)
			}
		})
	}

}

