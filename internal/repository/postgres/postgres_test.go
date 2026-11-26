package postgres_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"bulkdoc/db"
	"bulkdoc/internal/repository"
	"bulkdoc/internal/repository/postgres"
	"bulkdoc/internal/repository/repotest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

// sharedDSN is either DATABASE_URL or a DSN from a once-started testcontainers
// Postgres. One database + one schema per test keeps -count=10 isolation cheap.
var (
	sharedDSN  string
	sharedOnce sync.Once
	sharedErr  error
)

func databaseURL(t testing.TB) string {
	t.Helper()
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	sharedOnce.Do(func() {
		sharedDSN, sharedErr = startTestPostgres()
	})
	if sharedErr != nil {
		t.Fatalf("start testcontainers postgres: %v\n"+
			"hint: set DATABASE_URL, or run `docker compose run --rm test`", sharedErr)
	}
	return sharedDSN
}

func startTestPostgres() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("bulkdoc"),
		tcpostgres.WithUsername("bulkdoc"),
		tcpostgres.WithPassword("bulkdoc"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp").WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		return "", err
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(context.Background())
		return "", err
	}
	return dsn, nil
}

// newRepo creates an isolated schema and a GORM-backed repository pointed at it.
func newRepo(t testing.TB) (*postgres.Repo, *gorm.DB) {
	t.Helper()
	dsn := databaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	schema := "t_" + strings.ToLower(rand.Text()[:16])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	schemaDSN := dsn
	if strings.Contains(schemaDSN, "?") {
		schemaDSN += "&search_path=" + schema
	} else {
		schemaDSN += "?search_path=" + schema
	}

	gdb, err := postgres.Open(schemaDSN)
	if err != nil {
		admin.Close()
		t.Fatalf("gorm open: %v", err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		_, _ = admin.Exec(dctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})

	if _, err := sqlDB.ExecContext(ctx, db.Schema()); err != nil {
		t.Fatalf("apply db/schema.sql: %v", err)
	}

	repo, err := postgres.New(gdb)
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	return repo, gdb
}

func TestPostgresRepositoryContract(t *testing.T) {
	repotest.Run(t, func(t *testing.T) repository.Repository {
		repo, _ := newRepo(t)
		return repo
	})
}

func TestPostgresAvailable(t *testing.T) {
	dsn := databaseURL(t)
	gdb, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Logf("postgres ok (%s)", redactUserinfo(dsn))
}

func redactUserinfo(dsn string) string {
	if i := strings.Index(dsn, "@"); i >= 0 {
		if j := strings.Index(dsn, "://"); j >= 0 && j < i {
			return dsn[:j+3] + "***" + dsn[i:]
		}
	}
	return fmt.Sprintf("dsn(len=%d)", len(dsn))
}
