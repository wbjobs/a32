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
	lines      []string
	currentPos int64
}

func newSlowLogParser() *slowLogParser {
	return &slowLogParser{}
}

func (p *slowLogParser) feed(line string, pos int64) {
	if len(p.lines) == 0 {
		p.currentPos = pos
	}
	p.lines = append(p.lines, line)
}

func (p *slowLogParser) flush() (*pb.SlowLogEntry, int64) {
	if len(p.lines) == 0 {
		return nil, 0
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
	logPos := p.currentPos
	p.lines = p.lines[:0]

	if !enteredBlock || entry.Sql == "" {
		return nil, 0
	}
	return entry, logPos
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

type LineWithPos struct {
	text string
	pos  int64
}

func tailFile(ctx context.Context, path string, lines chan<- LineWithPos) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open slow log: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	pos := int64(0)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Text()
		lines <- LineWithPos{text: line, pos: pos}
		pos += int64(len(line)) + 1
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
		currentPos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek: %w", err)
		}
		scanner = bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
		for scanner.Scan() {
			lines <- LineWithPos{text: scanner.Text(), pos: pos}
			pos += int64(len(scanner.Text())) + 1
		}
		if _, err := f.Seek(currentPos, io.SeekStart); err != nil {
			return fmt.Errorf("seek back: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type Agent struct {
	serverAddr string
	agentID    string
	logPath    string
	cache      *LocalCache
	conn       *grpc.ClientConn
	stream     pb.SlowQueryService_StreamSlowLogClient
	sendCh     chan *pb.SlowLogEntry
	ackCh      chan *pb.Ack
	sequence   int32
}

func NewAgent(serverAddr, agentID, logPath string, cache *LocalCache) *Agent {
	return &Agent{
		serverAddr: serverAddr,
		agentID:    agentID,
		logPath:    logPath,
		cache:      cache,
		sendCh:     make(chan *pb.SlowLogEntry, 1024),
		ackCh:      make(chan *pb.Ack, 1024),
	}
}

func (a *Agent) connect(ctx context.Context) error {
	var err error
	a.conn, err = grpc.NewClient(a.serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	client := pb.NewSlowQueryServiceClient(a.conn)
	a.stream, err = client.StreamSlowLog(ctx)
	if err != nil {
		a.conn.Close()
		return fmt.Errorf("open stream: %w", err)
	}

	log.Printf("[agent] connected to %s", a.serverAddr)
	return nil
}

func (a *Agent) disconnect() {
	if a.stream != nil {
		a.stream.CloseSend()
	}
	if a.conn != nil {
		a.conn.Close()
	}
}

func (a *Agent) reconnectWithBackoff(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	attempts := 0

	for {
		attempts++
		err := a.connect(ctx)
		if err == nil {
			log.Printf("[agent] reconnected after %d attempts", attempts)
			a.resendPending(ctx)
			return nil
		}

		log.Printf("[agent] reconnect attempt %d failed: %v, backoff %v", attempts, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (a *Agent) resendPending(ctx context.Context) {
	pending := a.cache.GetAll()
	if len(pending) == 0 {
		return
	}
	log.Printf("[agent] resending %d pending entries", len(pending))
	for _, entry := range pending {
		select {
		case <-ctx.Done():
			return
		case a.sendCh <- entry:
		}
	}
}

func (a *Agent) runSender(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry := <-a.sendCh:
			if a.stream == nil {
				if err := a.reconnectWithBackoff(ctx); err != nil {
					return err
				}
			}

			if entry.GetUuid() == "" {
				entry.Uuid = GenerateEntryUUID(entry)
			}

			if err := a.cache.Add(entry); err != nil {
				log.Printf("[agent] cache add failed: %v", err)
			}

			entry.AgentId = a.agentID

			if err := a.stream.Send(entry); err != nil {
				log.Printf("[agent] send failed: %v, reconnecting...", err)
				a.disconnect()
				a.stream = nil
				if err := a.reconnectWithBackoff(ctx); err != nil {
					return err
				}
				continue
			}

			a.sequence++
		}
	}
}

func (a *Agent) runReceiver(ctx context.Context) error {
	for {
		if a.stream == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		ack, err := a.stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Printf("[agent] recv failed: %v, reconnecting...", err)
			a.disconnect()
			a.stream = nil
			if err := a.reconnectWithBackoff(ctx); err != nil {
				return err
			}
			continue
		}

		a.ackCh <- ack
	}
}

func (a *Agent) runAckHandler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-a.ackCh:
			if ack.GetUuid() != "" {
				if err := a.cache.Remove(ack.GetUuid()); err != nil {
					log.Printf("[agent] cache remove failed: %v", err)
				}
			}
			log.Printf("[ack] ok=%v seq=%d uuid=%s cache_size=%d msg=%s",
				ack.Ok, ack.Sequence, ack.Uuid, a.cache.Len(), ack.Message)
		}
	}
}

func (a *Agent) runLogTailer(ctx context.Context) error {
	lines := make(chan LineWithPos, 4096)
	go func() {
		if err := tailFile(ctx, a.logPath, lines); err != nil {
			log.Fatalf("tail failed: %v", err)
		}
	}()

	parser := newSlowLogParser()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case lp := <-lines:
			if strings.HasPrefix(lp.text, "# Time:") && len(parser.lines) > 0 {
				if entry, pos := parser.flush(); entry != nil {
					entry.LogOffset = pos
					entry.RawLine = strings.Join(parser.lines, "\n")
					entry.Uuid = GenerateEntryUUID(entry)
					select {
					case a.sendCh <- entry:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			parser.feed(lp.text, lp.pos)
		}
	}
}

func (a *Agent) Run(ctx context.Context) error {
	go a.runAckHandler(ctx)

	errCh := make(chan error, 3)
	go func() { errCh <- a.runSender(ctx) }()
	go func() { errCh <- a.runReceiver(ctx) }()
	go func() { errCh <- a.runLogTailer(ctx) }()

	select {
	case <-ctx.Done():
		a.disconnect()
		a.cache.Close()
		return ctx.Err()
	case err := <-errCh:
		a.disconnect()
		a.cache.Close()
		return err
	}
}

func main() {
	server := flag.String("server", "localhost:50051", "gRPC server address")
	logPath := flag.String("log", "", "Path to MySQL slow query log file")
	agentID := flag.String("id", "", "Agent identifier (defaults to hostname)")
	cacheDir := flag.String("cache-dir", "", "Local cache directory (defaults to ./.cache/<agent_id>)")
	maxCache := flag.Int("max-cache", 10000, "Maximum pending entries in local cache")
	flag.Parse()

	if *logPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: agent -log <slow_query_log_path> [-server <addr>] [-id <agent_id>] [-cache-dir <dir>] [-max-cache <n>]")
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

	cacheDirPath := *cacheDir
	if cacheDirPath == "" {
		cacheDirPath = filepath.Join(".cache", id)
	}

	cache, err := NewLocalCache(cacheDirPath, *maxCache)
	if err != nil {
		log.Fatalf("init cache: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("agent %s starting, tailing %s -> %s, cache_dir=%s, cached_pending=%d",
		id, absPath, *server, cacheDirPath, cache.Len())

	agent := NewAgent(*server, id, absPath, cache)
	if err := agent.Run(ctx); err != nil {
		log.Fatalf("agent error: %v", err)
	}
}
