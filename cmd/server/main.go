package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
	"github.com/trae3/slowquery-lineage/server"
	"google.golang.org/grpc"
)

func main() {
	grpcAddr := flag.String("grpc-addr", ":50051", "gRPC listen address")
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	window := flag.Duration("window", 1*time.Hour, "sliding window duration for heat map")
	maxSize := flag.Int("max-events", 100000, "maximum events to keep in memory")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address for idempotency")
	redisPassword := flag.String("redis-password", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis DB index")
	redisTTL := flag.Duration("redis-ttl", 24*time.Hour, "Redis idempotency key TTL")
	noRedis := flag.Bool("no-redis", false, "Disable Redis idempotency (for testing)")
	flag.Parse()

	heatMap := server.NewHeatMap(*window, *maxSize)

	var idempot *server.IdempotencyStore
	if !*noRedis {
		idempot = server.NewIdempotencyStore(*redisAddr, *redisPassword, *redisDB, *redisTTL)
		if err := idempot.Ping(nil); err != nil {
			log.Printf("[warn] Redis not reachable at %s: %v, idempotency disabled", *redisAddr, err)
			idempot = nil
		} else {
			log.Printf("[idempotency] Redis enabled at %s, TTL=%v", *redisAddr, *redisTTL)
		}
	} else {
		log.Printf("[idempotency] Redis disabled")
	}

	grpcService := server.NewGRPCService(heatMap, idempot)
	httpHandler := server.NewHTTPHandler(heatMap)

	grpcServer := grpc.NewServer()
	pb.RegisterSlowQueryServiceServer(grpcServer, grpcService)

	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen gRPC: %v", err)
	}

	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: httpHandler.Handler(),
	}

	go func() {
		log.Printf("gRPC server listening on %s", *grpcAddr)
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Fatalf("serve gRPC: %v", err)
		}
	}()

	go func() {
		log.Printf("HTTP server listening on %s", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve HTTP: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	grpcServer.GracefulStop()
	httpServer.Close()
	if idempot != nil {
		idempot.Close()
	}
}
