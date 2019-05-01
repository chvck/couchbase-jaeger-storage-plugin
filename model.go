package main

import (
	"time"

	"github.com/jaegertracing/jaeger/model"
)

type TraceID struct {
	High uint64 `json:"hi"`
	Low  uint64 `json:"lo"`
}

type SpanRef struct {
	TraceID TraceID `json:"trace_id"`
	SpanID  uint64  `json:"span_id"`
	RefType int32   `json:"ref_type"`
}

type Span struct {
	OperationName string           `json:"operation_name,omitempty"`
	References    []SpanRef        `json:"references"`
	Flags         model.Flags      `json:"flags"`
	StartTime     string           `json:"start_time"` // this is necessary until an analytics issue is closed out.
	Duration      time.Duration    `json:"duration"`
	Tags          []model.KeyValue `json:"tags"`
	Logs          []model.Log      `json:"logs"`
	Process       *model.Process   `json:"process,omitempty"`
	ProcessID     string           `json:"process_id,omitempty"`
	Warnings      []string         `json:"warnings,omitempty"`
	TraceID       TraceID          `json:"trace_id"`
	SpanID        uint64           `json:"span_id"`
	Type          string           `json:"type"`
	ProcessedTags []string         `json:"processed_tags"`
}

type Tag struct {
	Tag string `json:"tag"`
}

type TagInsertion struct {
	TagKey   string `json:"tag_key"`
	TagValue string `json:"tag_value"`
}

type UniqueTraceIDs map[TraceID]struct{}

func traceIDToDomain(traceID TraceID) model.TraceID {
	var modelTraceID model.TraceID
	modelTraceID.High = traceID.High
	modelTraceID.Low = traceID.Low
	return modelTraceID
}

func traceIDFromDomain(traceID model.TraceID) TraceID {
	var dbTraceID TraceID
	dbTraceID.High = traceID.High
	dbTraceID.Low = traceID.Low
	return dbTraceID
}

// Add adds a traceID to the existing map
func (u UniqueTraceIDs) Add(traceID TraceID) {
	u[traceID] = struct{}{}
}

func (s *Span) toDomain() (*model.Span, error) {
	startTime, err := time.Parse(dateLayout, s.StartTime)
	if err != nil {
		return nil, err
	}
	modelSpan := &model.Span{
		TraceID:       traceIDToDomain(s.TraceID),
		SpanID:        model.SpanID(s.SpanID),
		Duration:      s.Duration,
		StartTime:     startTime,
		OperationName: s.OperationName,
		ProcessID:     s.ProcessID,
		Flags:         s.Flags,
		Logs:          s.Logs,
		Warnings:      s.Warnings,
		Tags:          s.Tags,
	}
	modelSpan.Process = &model.Process{
		ServiceName: s.Process.ServiceName,
		Tags:        s.Process.Tags,
	}
	for _, ref := range s.References {
		modelSpan.References = append(modelSpan.References, model.SpanRef{
			SpanID:  model.SpanID(ref.SpanID),
			TraceID: traceIDToDomain(ref.TraceID),
			RefType: model.SpanRefType(ref.RefType),
		})
	}

	return modelSpan, nil
}
