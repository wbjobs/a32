package server

import (
	"io"
	"log"
	"strconv"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
)

type GRPCService struct {
	pb.UnimplementedSlowQueryServiceServer
	heatMap *HeatMap
}

func NewGRPCService(heatMap *HeatMap) *GRPCService {
	return &GRPCService{
		heatMap: heatMap,
	}
}

func (s *GRPCService) StreamSlowLog(stream pb.SlowQueryService_StreamSlowLogServer) error {
	seq := int32(0)
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return stream.Send(&pb.Ack{
				Ok:       true,
				Message:  "stream closed",
				Sequence: seq,
			})
		}
		if err != nil {
			return err
		}

		events := s.processEntry(entry)
		for _, event := range events {
			s.heatMap.Add(event)
		}

		seq++
		if err := stream.Send(&pb.Ack{
			Ok:       true,
			Message:  "processed",
			Sequence: seq,
		}); err != nil {
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
			"[lineage] agent=%s table=%s type=%s fields=%v qt=%.3f lt=%.3f",
			entry.GetAgentId(),
			table,
			lineage.SQLType,
			lineage.Fields,
			entry.GetQueryTime(),
			entry.GetLockTime(),
		)
	}

	return events
}

func parseIntField(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func parseFloatField(s string) float64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseFloat(s, 64)
	return n
}
