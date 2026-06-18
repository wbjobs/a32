package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
	_ "modernc.org/sqlite"
)

type SlowQueryRecord struct {
	ID         int64
	UUID       string
	AgentID    string
	TableName  string
	SQL        string
	Database   string
	ClientIP   string
	ThreadID   int64
	QueryTime  float64
	LockTime   float64
	RowsSent   int64
	RowsExamined int64
	Timestamp  time.Time
	SQLType    string
}

type Storage struct {
	db *sql.DB
}

func NewStorage(dbPath string) (*Storage, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &Storage{db: db}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Storage) initSchema() error {
	stmt := `
	CREATE TABLE IF NOT EXISTS slow_queries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		uuid TEXT NOT NULL UNIQUE,
		agent_id TEXT NOT NULL,
		table_name TEXT NOT NULL,
		sql_text TEXT NOT NULL,
		database_name TEXT,
		client_ip TEXT,
		thread_id INTEGER,
		query_time REAL NOT NULL,
		lock_time REAL NOT NULL,
		rows_sent INTEGER,
		rows_examined INTEGER,
		timestamp DATETIME NOT NULL,
		sql_type TEXT,
		minute_bucket TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_slow_queries_table_time ON slow_queries(table_name, timestamp);
	CREATE INDEX IF NOT EXISTS idx_slow_queries_minute_bucket ON slow_queries(minute_bucket);
	CREATE INDEX IF NOT EXISTS idx_slow_queries_timestamp ON slow_queries(timestamp);
	CREATE INDEX IF NOT EXISTS idx_slow_queries_table_minute ON slow_queries(table_name, minute_bucket);
	`
	_, err := s.db.Exec(stmt)
	return err
}

func minuteBucket(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04")
}

func (s *Storage) SaveSlowQueries(ctx context.Context, records []SlowQueryRecord) error {
	if len(records) == 0 {
		return nil
	}

	valuePlaceholders := make([]string, 0, len(records))
	args := make([]interface{}, 0, len(records)*14)

	for _, r := range records {
		valuePlaceholders = append(valuePlaceholders, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		args = append(args,
			r.UUID,
			r.AgentID,
			r.TableName,
			r.SQL,
			r.Database,
			r.ClientIP,
			r.ThreadID,
			r.QueryTime,
			r.LockTime,
			r.RowsSent,
			r.RowsExamined,
			r.Timestamp.UTC(),
			r.SQLType,
			minuteBucket(r.Timestamp),
		)
	}

	query := `INSERT OR IGNORE INTO slow_queries (
		uuid, agent_id, table_name, sql_text, database_name, client_ip, thread_id,
		query_time, lock_time, rows_sent, rows_examined, timestamp, sql_type, minute_bucket
	) VALUES ` + strings.Join(valuePlaceholders, ",")

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *Storage) GetSlowQueriesByTable(ctx context.Context, tableName string, start, end time.Time, limit int) ([]SlowQueryRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, uuid, agent_id, table_name, sql_text, database_name,
		       client_ip, thread_id, query_time, lock_time, rows_sent,
		       rows_examined, timestamp, sql_type
		FROM slow_queries
		WHERE table_name = ? AND timestamp >= ? AND timestamp <= ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, tableName, start.UTC(), end.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []SlowQueryRecord
	for rows.Next() {
		var r SlowQueryRecord
		var ts time.Time
		err := rows.Scan(
			&r.ID, &r.UUID, &r.AgentID, &r.TableName, &r.SQL,
			&r.Database, &r.ClientIP, &r.ThreadID, &r.QueryTime,
			&r.LockTime, &r.RowsSent, &r.RowsExamined, &ts, &r.SQLType,
		)
		if err != nil {
			return nil, err
		}
		r.Timestamp = ts
		records = append(records, r)
	}
	return records, rows.Err()
}

type PerTableMinuteCount struct {
	TableName   string
	MinuteBucket string
	Count       int
}

func (s *Storage) GetCountsPerTablePerMinute(ctx context.Context, tableName string, since time.Time) ([]PerTableMinuteCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT table_name, minute_bucket, COUNT(*) as cnt
		FROM slow_queries
		WHERE table_name = ? AND timestamp >= ?
		GROUP BY table_name, minute_bucket
		ORDER BY minute_bucket
	`, tableName, since.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PerTableMinuteCount
	for rows.Next() {
		var r PerTableMinuteCount
		if err := rows.Scan(&r.TableName, &r.MinuteBucket, &r.Count); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Storage) GetAllTableCountsPerMinute(ctx context.Context, since time.Time) (map[string][]PerTableMinuteCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT table_name, minute_bucket, COUNT(*) as cnt
		FROM slow_queries
		WHERE timestamp >= ?
		GROUP BY table_name, minute_bucket
		ORDER BY table_name, minute_bucket
	`, since.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]PerTableMinuteCount)
	for rows.Next() {
		var r PerTableMinuteCount
		if err := rows.Scan(&r.TableName, &r.MinuteBucket, &r.Count); err != nil {
			return nil, err
		}
		result[r.TableName] = append(result[r.TableName], r)
	}
	return result, rows.Err()
}

func (s *Storage) CleanOldData(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM slow_queries WHERE timestamp < ?`, olderThan.UTC())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func RecordsFromEntry(entry *pb.SlowLogEntry, tables []string, sqlType string, ts time.Time) []SlowQueryRecord {
	records := make([]SlowQueryRecord, 0, len(tables))
	for _, table := range tables {
		records = append(records, SlowQueryRecord{
			UUID:         entry.GetUuid(),
			AgentID:      entry.GetAgentId(),
			TableName:    table,
			SQL:          entry.GetSql(),
			Database:     entry.GetDatabase(),
			ClientIP:     entry.GetClientIp(),
			ThreadID:     entry.GetThreadId(),
			QueryTime:    entry.GetQueryTime(),
			LockTime:     entry.GetLockTime(),
			RowsSent:     entry.GetRowsSent(),
			RowsExamined: entry.GetRowsExamined(),
			Timestamp:    ts,
			SQLType:      sqlType,
		})
	}
	return records
}
