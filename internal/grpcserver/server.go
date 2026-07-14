package grpcserver

import (
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	heliosv1 "github.com/helios/proto/gen/proto"
	"github.com/helios/internal/buffer"
	"github.com/helios/internal/metrics"
	"github.com/helios/pkg/event"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const alertChannel = "helios:alerts"

type Server struct {
	heliosv1.UnimplementedHeliosServiceServer
	rb          *buffer.RingBuffer[event.Event]
	redisClient *redis.Client
	log         zerolog.Logger
}

func New(rb *buffer.RingBuffer[event.Event], redisClient *redis.Client, log zerolog.Logger) *Server {
	return &Server{rb: rb, redisClient: redisClient, log: log}
}

// IngestStream receives a client-side stream of events and enqueues each into
// the ring buffer. Returns a single summary response when the client closes.
func (s *Server) IngestStream(stream heliosv1.HeliosService_IngestStreamServer) error {
	accepted, dropped := 0, 0

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&heliosv1.IngestResponse{
				Accepted: true,
				Reason:   fmt.Sprintf("accepted=%d dropped=%d", accepted, dropped),
			})
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv: %v", err)
		}

		ev := event.Event{
			ID:        uuid.New().String(),
			Source:    event.Source(req.Source),
			Level:     event.Level(req.Level),
			Message:   req.Message,
			Timestamp: time.Now(),
			Tags:      make([]string, 0, len(req.Tags)),
		}
		for k, v := range req.Tags {
			ev.Tags = append(ev.Tags, k+"="+v)
		}

		if err := s.rb.Enqueue(ev); err != nil {
			dropped++
			s.log.Warn().Str("event_id", ev.ID).Msg("gRPC ingest: buffer full, dropping event")
		} else {
			accepted++
			metrics.EventsIngested.WithLabelValues(req.Source, req.Level).Inc()
		}
	}
}

// WatchAlerts subscribes to Redis pub/sub and streams anomaly alerts to the
// caller until the client disconnects or the context is cancelled.
func (s *Server) WatchAlerts(req *heliosv1.WatchRequest, stream heliosv1.HeliosService_WatchAlertsServer) error {
	ctx := stream.Context()

	sourceFilter := make(map[string]struct{}, len(req.Sources))
	for _, src := range req.Sources {
		sourceFilter[src] = struct{}{}
	}

	sub := s.redisClient.Subscribe(ctx, alertChannel)
	defer sub.Close()

	s.log.Info().
		Strs("sources", req.Sources).
		Float64("min_score", req.MinAnomalyScore).
		Msg("gRPC WatchAlerts: client subscribed")

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return status.Error(codes.Unavailable, "alert channel closed")
			}

			alert, err := parseAlert(msg.Payload)
			if err != nil {
				s.log.Error().Err(err).Msg("WatchAlerts: failed to parse alert")
				continue
			}

			// Apply source filter.
			if len(sourceFilter) > 0 {
				if _, ok := sourceFilter[alert.Source]; !ok {
					continue
				}
			}
			// Apply score filter.
			if alert.AnomalyScore < req.MinAnomalyScore {
				continue
			}

			if err := stream.Send(alert); err != nil {
				return status.Errorf(codes.Internal, "send: %v", err)
			}
		}
	}
}
