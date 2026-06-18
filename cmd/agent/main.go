package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	reTimestamp  = regexp.MustCompile(`^#\s+Time:\s+(\S+)`)
	reUserHost   = regexp.MustCompile(`^#\s+User@Host:`)
	reQueryTime  = regexp.MustCompile(`^#\s+Query_time:\s+([\d.]+)\s+Lock_time:\s+([\d.]+)\s+Rows_sent:\s+(\d+)\s+Rows_examined:\s+(\d+)`)
	reSlowLogHdr = regexp.MustCompile(`^/.*\s+MySQL,\s+Version:`)
)

type slowLogParser struct {
	lines []string
}

func newSlowLogParser() *slowLogParser {
	return &slowLogParser{}
}

func (p *slowLogParser) feed(line string) {
	p.lines = append(p.lines, line)
}

func (p *slowLogParser) flush() *pb.SlowLogEntry {
	if len(p.lines) == 0 {
		return nil
	}
	entry := &pb.SlowLogEntry{}
	enteredBlock := false
	var sqlParts []string
	var currentDB string

	for _, line := range p.lines {
		if m := reTimestamp.FindStringSubmatch(line); m != nil {
			ts, err := parseMySQLTimestamp(m[1])
			if err == nil {
				entry.Timestamp = ts
			}
			enteredBlock = true
			continue
		}
		if reUserHost.MatchString(line) {
			enteredBlock = true
			continue
		}
		if m := reQueryTime.FindStringSubmatch(line); m != nil {
			if qt, err := strconv.ParseFloat(m[1], 64); err == nil {
				entry.QueryTime = qt
			}
			if lt, err := strconv.ParseFloat(m[2], 64); err == nil {
				entry.LockTime = lt
			}
			if rs, err := strconv.ParseInt(m[3], 10, 64); err == nil {
				entry.RowsSent = rs
			}
			if re, err := strconv.ParseInt(m[4], 10, 64); err == nil {
				entry.RowsExamined = re
			}
			enteredBlock = true
			continue
		}
		if reSlowLogHdr.MatchString(line) {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if enteredBlock && line != "" {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "use ") {
				parts := strings.SplitN(trimmed, " ", 3)
				if len(parts) >= 2 {
					currentDB = strings.TrimRight(parts[1], ";")
				}
				if len(parts) >= 3 {
					sqlParts = append(sqlParts, parts[2])
				}
				continue
			}
			if strings.HasPrefix(trimmed, "SET ") {
				continue
			}
			sqlParts = append(sqlParts, trimmed)
		}
	}

	if len(sqlParts) > 0 {
		entry.Sql = strings.Join(sqlParts, " ")
	}
	if currentDB != "" {
		entry.Database = currentDB
	}
	p.lines = p.lines[:0]

	if !enteredBlock || entry.Sql == "" {
		return nil
	}
	return entry
}

func parseMySQLTimestamp(s string) (string, error) {
	s = strings.TrimRight(s, "UTC")
	s = strings.TrimSpace(s)
	formats := []string{
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02 15:04:05.000000",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
	}
	return "", fmt.Errorf("cannot parse timestamp: %s", s)
}

func tailFile(ctx context.Context, path string, lines chan<- string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open slow log: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		lines <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan slow log: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek: %w", err)
		}
		scanner = bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			return fmt.Errorf("seek back: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func runAgent(ctx context.Context, serverAddr, logPath, agentID string) error {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	client := pb.NewSlowQueryServiceClient(conn)
	stream, err := client.StreamSlowLog(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer stream.CloseSend()

	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				return
			}
			log.Printf("[ack] ok=%v seq=%d msg=%s", ack.Ok, ack.Sequence, ack.Message)
		}
	}()

	lines := make(chan string, 4096)
	go func() {
		if err := tailFile(ctx, logPath, lines); err != nil {
			log.Fatalf("tail failed: %v", err)
		}
	}()

	parser := newSlowLogParser()
	seq := int32(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case line := <-lines:
			if strings.HasPrefix(line, "# Time:") && len(parser.lines) > 0 {
				if entry := parser.flush(); entry != nil {
					entry.AgentId = agentID
					entry.RawLine = strings.Join(parser.lines, "\n")
					if err := stream.Send(entry); err != nil {
						return fmt.Errorf("send: %w", err)
					}
					seq++
				}
			}
			parser.feed(line)
		}
	}
}

func main() {
	server := flag.String("server", "localhost:50051", "gRPC server address")
	logPath := flag.String("log", "", "Path to MySQL slow query log file")
	agentID := flag.String("id", "", "Agent identifier (defaults to hostname)")
	flag.Parse()

	if *logPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: agent -log <slow_query_log_path> [-server <addr>] [-id <agent_id>]")
		os.Exit(1)
	}

	absPath, err := filepath.Abs(*logPath)
	if err != nil {
		log.Fatalf("resolve log path: %v", err)
	}

	id := *agentID
	if id == "" {
		hostname, _ := os.Hostname()
		id = hostname
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("agent %s starting, tailing %s -> %s", id, absPath, *server)
	if err := runAgent(ctx, *server, absPath, id); err != nil {
		log.Fatalf("agent error: %v", err)
	}
}
