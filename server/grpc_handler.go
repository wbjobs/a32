package server

import (
	"io"
	"log"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
)

type GRPCService struct {
	pb.UnimplementedSlowQueryServiceServer
	heatMap *HeatMap
	idempot *IdempotencyStore
}

func NewGRPCService(heatMap *HeatMap, idempot *IdempotencyStore) *GRPCService {
	return &GRPCService{
		heatMap: heatMap,
		idempot: idempot,
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
			Ok:       true,
			Message:  "processed",
			Sequence: seq,
			Uuid:     uuid,
			LogOffset: entry.GetLogOffset(),
		}

		if s.idempot != nil && uuid != "" {
			isNew, err := s.idempot.CheckAndSet(ctx, uuid)
			if err != nil {
				log.Printf("[idempot] error checking uuid %s: %v", uuid, err)
				ack.Ok = false
				ack.Message = "idempotency check failed"
			} else if !isNew {
				log.Printf("[idempot] duplicate entry skipped, uuid=%s", uuid)
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
			"[lineage] agent=%s table=%s type=%s fields=%v qt=%.3f lt=%.3f uuid=%s",
			entry.GetAgentId(),
			table,
			lineage.SQLType,
			lineage.Fields,
			entry.GetQueryTime(),
			entry.GetLockTime(),
			entry.GetUuid(),
		)
	}

	return events
}
