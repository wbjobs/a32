package server

import (
	"io"
	"log"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
)

type GRPCService struct {
	pb.UnimplementedSlowQueryServiceServer
	heatMap  *HeatMap
	idempot  *IdempotencyStore
	storage  *Storage
	counter  *PerMinuteCounter
}

func NewGRPCService(heatMap *HeatMap, idempot *IdempotencyStore, storage *Storage, counter *PerMinuteCounter) *GRPCService {
	return &GRPCService{
		heatMap: heatMap,
		idempot: idempot,
		storage: storage,
		counter: counter,
	}
}

func (s *GRPCService) StreamSlowLog(stream pb.SlowQueryService_StreamSlowLogServer) error {
	seq := int32(0)
	ctx := stream.Context()
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		uuid := entry.GetUuid()
		ack := &pb.Ack{
			Ok:        true,
			Message:   "processed",
			Sequence:  seq,
			Uuid:      uuid,
			LogOffset: entry.GetLogOffset(),
		}

		isNew := true
		if s.idempot != nil && uuid != "" {
			var err error
			isNew, err = s.idempot.CheckAndSet(ctx, uuid)
			if err != nil {
				log.Printf("[idempot] error checking uuid %s: %v", uuid, err)
				ack.Ok = false
				ack.Message = "idempotency check failed"
			} else if !isNew {
				ack.Ok = true
				ack.Message = "duplicate skipped"
				if err := stream.Send(ack); err != nil {
					return err
				}
				seq++
				continue
			}
		}

		events := s.processEntry(entry)
		for _, event := range events {
			s.heatMap.Add(event)
			if s.counter != nil {
				s.counter.Record(event.TableName)
			}
		}

		if s.storage != nil && isNew && len(events) > 0 {
			tables := make([]string, 0, len(events))
			sqlType := ""
			for _, e := range events {
				tables = append(tables, e.TableName)
				if sqlType == "" {
					sqlType = e.SQLType
				}
			}
			ts := events[0].Timestamp
			records := RecordsFromEntry(entry, tables, sqlType, ts)
			if err := s.storage.SaveSlowQueries(ctx, records); err != nil {
				log.Printf("[storage] save failed: %v", err)
			}
		}

		seq++
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (s *GRPCService) processEntry(entry *pb.SlowLogEntry) []HeatEvent {
	sql := entry.GetSql()
	if sql == "" {
		return nil
	}

	lineage := ExtractLineage(sql)
	if len(lineage.Tables) == 0 {
		return nil
	}

	var ts time.Time
	if entry.GetTimestamp() != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, entry.GetTimestamp()); err == nil {
			ts = parsed
		}
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	events := make([]HeatEvent, 0, len(lineage.Tables))
	for _, table := range lineage.Tables {
		evt := HeatEvent{
			TableName: table,
			QueryTime: entry.GetQueryTime(),
			LockTime:  entry.GetLockTime(),
			Timestamp: ts,
			Fields:    lineage.Fields,
			SQLType:   lineage.SQLType,
		}
		events = append(events, evt)
		log.Printf(
			"[lineage] agent=%s table=%s type=%s fields=%v qt=%.3f lt=%.3f uuid=%s ip=%s tid=%d",
			entry.GetAgentId(),
			table,
			lineage.SQLType,
			lineage.Fields,
			entry.GetQueryTime(),
			entry.GetLockTime(),
			entry.GetUuid(),
			entry.GetClientIp(),
			entry.GetThreadId(),
		)
	}

	return events
}
