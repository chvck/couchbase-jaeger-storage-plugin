package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	ottag "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
)

var (
	querySpanByTraceID = `
SELECT trace_id, span_id, operation_name, flags, start_time, duration, tags, logs, references, process
FROM %s
WHERE trace_id.hi = ? AND trace_id.lo = ? AND ` + "`type`" + `="span"`
	queryServiceNames   = `SELECT DISTINCT process.service_name from %s where ` + "`type`" + `="span"`
	queryOperationNames = `SELECT DISTINCT operation_name from %s where process.service_name = ? AND ` + "`type`" + `="span"`
	queryIDsByTag       = `
SELECT DISTINCT RAW b.trace_id
FROM %s AS b
WHERE b.process.service_name = ? AND b.start_time > ? AND b.start_time < ? AND ` + "b.`type`" + `="span" AND (EVERY tag IN ? SATISFIES tag IN b.processed_tags END)
ORDER BY b.start_time DESC
LIMIT ?`
	queryIDsByServiceName = `
SELECT DISTINCT RAW sb.trace_id
FROM %s sb
WHERE sb.process.service_name = ? AND sb.start_time > ? AND sb.start_time < ? AND ` + "sb.`type`" + `="span"
ORDER BY sb.start_time DESC
LIMIT ?`
	queryIDsByServiceAndOperationName = `
SELECT DISTINCT RAW trace_id
FROM %s AS b
WHERE process.service_name = ? AND operation_name = ? AND start_time > ? AND start_time < ? AND` + "`type`" + `="span"
ORDER BY start_time DESC
LIMIT ?`
	queryIDsByServiceAndOperationNameAndTags = `
SELECT DISTINCT RAW trace_id
FROM %s AS b
WHERE process.service_name = ? AND operation_name = ? AND start_time > ? AND start_time < ? AND` + "`type`" + `="span"
AND (EVERY tag IN ? SATISFIES tag IN b.processed_tags END)
ORDER BY start_time DESC
LIMIT ?`
	queryIDsByDuration = `
SELECT DISTINCT RAW trace_id
FROM %s AS b
WHERE process.service_name = ? AND duration > ? AND duration < ? AND ` + "`type`" + `="span"
LIMIT ?`
	queryIDsByDurationAndOperationName = `
SELECT DISTINCT RAW trace_id
FROM %s AS b
WHERE process.service_name = ? AND operation_name = ? AND duration > ? AND duration < ? AND ` + "`type`" + `="span"
LIMIT ?`

	queryTracesBySubQuery = `
SELECT b.trace_id, b.span_id, b.operation_name, b.flags, b.start_time, b.duration, b.tags, b.logs, b.references, b.process
FROM %s b
WHERE b.trace_id IN (%s)
ORDER BY b.trace_id, b.start_time`

	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = errors.New("service Name must be set")

	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = errors.New("start Time Minimum is above Maximum")

	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = errors.New("duration Minimum is above Maximum")

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = errors.New("malformed request object")

	// ErrDurationAndTagQueryNotSupported occurs when duration and tags are both set
	ErrDurationAndTagQueryNotSupported = errors.New("cannot query for duration and tags simultaneously")

	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = errors.New("start and End Time must be set")
)

const (
	defaultNumTraces = 100
)

type couchbaseSpanReader struct {
	store Store
}

func (cs *couchbaseSpanReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := cs.startSpanForQuery(ctx, "readTrace", querySpanByTraceID)
	defer span.Finish()
	span.LogFields(otlog.String("event", "searching"), otlog.Object("trace_id", traceID))

	dbTraceID := traceIDFromDomain(traceID)
	result, err := cs.store.Query(querySpanByTraceID, []interface{}{dbTraceID.High, dbTraceID.Low})
	if err != nil {
		cs.logErrorToSpan(span, err)
		return nil, err
	}

	var trace model.Trace
	var traceSpan Span
	for result.Next(&traceSpan) {
		span, err := traceSpan.toDomain()
		if err != nil {
			return nil, err
		}
		trace.Spans = append(trace.Spans, span)
	}

	err = result.Close()
	if err != nil {
		return nil, errors.Wrap(err, "Error reading traces from storage")
	}
	if len(trace.Spans) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return &trace, err
}

func (cs *couchbaseSpanReader) GetServices(ctx context.Context) ([]string, error) {
	result, err := cs.store.Query(queryServiceNames, nil)
	if err != nil {
		return nil, err
	}

	var serviceName struct {
		ServiceName string `json:"service_name"`
	}
	var serviceNames []string
	for result.Next(&serviceName) {
		if serviceName.ServiceName != "" {
			serviceNames = append(serviceNames, serviceName.ServiceName)
		}
	}

	err = result.Close()
	if err != nil {
		return nil, err
	}

	return serviceNames, nil
}

func (cs *couchbaseSpanReader) GetOperations(ctx context.Context, service string) ([]string, error) {
	result, err := cs.store.Query(queryOperationNames, []interface{}{service})
	if err != nil {
		return nil, err
	}

	var operationName struct {
		OperationName string `json:"operation_name"`
	}
	var operationNames []string
	for result.Next(&operationName) {
		if operationName.OperationName != "" {
			operationNames = append(operationNames, operationName.OperationName)
		}
	}

	err = result.Close()
	if err != nil {
		return nil, err
	}

	return operationNames, nil
}

func (cs *couchbaseSpanReader) FindTraces(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	if err := cs.validateQuery(traceQuery); err != nil {
		return nil, err
	}

	if traceQuery.NumTraces == 0 {
		traceQuery.NumTraces = defaultNumTraces
	}

	return cs.findTraces(ctx, traceQuery)
}

func (cs *couchbaseSpanReader) FindTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	if err := cs.validateQuery(traceQuery); err != nil {
		return nil, err
	}

	if traceQuery.NumTraces == 0 {
		traceQuery.NumTraces = defaultNumTraces
	}

	dbTraceIDs, err := cs.findTraceIDs(ctx, traceQuery)
	if err != nil {
		return nil, err
	}

	var traceIDs []model.TraceID
	for t := range dbTraceIDs {
		if len(traceIDs) >= traceQuery.NumTraces {
			break
		}
		traceIDs = append(traceIDs, traceIDToDomain(t))
	}
	return traceIDs, nil
}

func (cs *couchbaseSpanReader) findTraces(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	if traceQuery.DurationMin != 0 || traceQuery.DurationMax != 0 {
		return cs.queryTracesByDuration(ctx, traceQuery)
	}

	if traceQuery.OperationName != "" {
		if len(traceQuery.Tags) > 0 {
			return cs.queryTracesByServiceNameAndOperationAndTagsAndLogs(ctx, traceQuery)
		}

		return cs.queryTracesByServiceNameAndOperation(ctx, traceQuery)
	}
	if len(traceQuery.Tags) > 0 {
		return cs.queryTracesByTagsAndLogs(ctx, traceQuery)
	}
	return cs.queryTracesByService(ctx, traceQuery)
}

func (cs *couchbaseSpanReader) queryTracesByService(ctx context.Context, tq *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	queryStmt := fmt.Sprintf(queryTracesBySubQuery, cs.store.Name(), queryIDsByServiceName)
	span, ctx := cs.startSpanForQuery(ctx, "queryTracesByService", queryStmt)
	defer span.Finish()

	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}

	return cs.executeTraceQuery(span, queryStmt, params)
}

func (cs *couchbaseSpanReader) queryTracesByServiceNameAndOperationAndTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	queryStmt := fmt.Sprintf(queryTracesBySubQuery, cs.store.Name(), queryIDsByServiceAndOperationNameAndTags)
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByServiceAndOperationNameAndTags", queryStmt)
	defer span.Finish()

	var where []string
	for k, v := range tq.Tags {
		where = append(where, fmt.Sprintf("%s_%s", k, v))
	}

	params := []interface{}{
		tq.ServiceName,
		tq.OperationName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		where,
		tq.NumTraces,
	}

	return cs.executeTraceQuery(span, queryStmt, params)
}

func (cs *couchbaseSpanReader) queryTracesByTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	queryStmt := fmt.Sprintf(queryTracesBySubQuery, cs.store.Name(), queryIDsByTag)
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByTagsAndLogs", queryStmt)
	defer span.Finish()

	var where []string
	for k, v := range tq.Tags {
		where = append(where, fmt.Sprintf("%s_%s", k, v))
	}

	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		where,
		tq.NumTraces,
	}

	return cs.executeTraceQuery(span, queryStmt, params)
}

func (cs *couchbaseSpanReader) queryTracesByDuration(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	var queryStmt string
	if traceQuery.OperationName == "" {
		queryStmt = fmt.Sprintf(queryTracesBySubQuery, cs.store.Name(), queryIDsByDuration)
	} else {
		queryStmt = fmt.Sprintf(queryTracesBySubQuery, cs.store.Name(), queryIDsByDurationAndOperationName)
	}
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByDuration", queryStmt)
	defer span.Finish()

	minDuration := traceQuery.DurationMin.Nanoseconds()
	maxDuration := (time.Hour * 24).Nanoseconds()
	if traceQuery.DurationMax != 0 {
		maxDuration = traceQuery.DurationMax.Nanoseconds()
	}

	var params []interface{}
	if traceQuery.OperationName == "" {
		params = []interface{}{
			traceQuery.ServiceName,
			minDuration,
			maxDuration,
			traceQuery.NumTraces,
		}
	} else {
		params = []interface{}{
			traceQuery.ServiceName,
			traceQuery.OperationName,
			minDuration,
			maxDuration,
			traceQuery.NumTraces,
		}
	}

	return cs.executeTraceQuery(span, queryStmt, params)
}

func (cs *couchbaseSpanReader) queryTracesByServiceNameAndOperation(ctx context.Context, tq *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	queryStmt := fmt.Sprintf(queryTracesBySubQuery, cs.store.Name(), queryIDsByServiceAndOperationName)
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByServiceAndOperationName", queryStmt)
	defer span.Finish()

	params := []interface{}{
		tq.ServiceName,
		tq.OperationName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeTraceQuery(span, queryStmt, params)
}

func (cs *couchbaseSpanReader) executeTraceQuery(span opentracing.Span, query string, params []interface{}) ([]*model.Trace, error) {
	result, err := cs.store.Query(query, params)
	if err != nil {
		cs.logErrorToSpan(span, err)
		return nil, err
	}

	var traceSpan Span
	var trace *model.Trace
	var traces []*model.Trace
	var traceID TraceID
	for result.Next(&traceSpan) {
		if traceID != traceSpan.TraceID {
			traceID = traceSpan.TraceID
			trace = &model.Trace{}
			traces = append(traces, trace)
		}

		span, err := traceSpan.toDomain()
		if err != nil {
			return nil, err
		}

		trace.Spans = append(trace.Spans, span)
	}

	err = result.Close()
	if err != nil {
		cs.logErrorToSpan(span, err)
		return nil, err
	}

	return traces, nil
}

func (cs *couchbaseSpanReader) findTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	if traceQuery.DurationMin != 0 || traceQuery.DurationMax != 0 {
		return cs.queryIDsByDuration(ctx, traceQuery)
	}

	if traceQuery.OperationName != "" {
		if len(traceQuery.Tags) > 0 {
			return cs.queryIDsByServiceNameAndOperationAndTagsAndLogs(ctx, traceQuery)
		}

		return cs.queryIDsByServiceNameAndOperation(ctx, traceQuery)
	}
	if len(traceQuery.Tags) > 0 {
		return cs.queryIDsByTagsAndLogs(ctx, traceQuery)
	}
	return cs.queryIDsByService(ctx, traceQuery)
}

func (cs *couchbaseSpanReader) queryIDsByServiceNameAndOperationAndTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByServiceAndOperationNameAndTags", queryIDsByServiceAndOperationNameAndTags)
	defer span.Finish()

	var where []string
	for k, v := range tq.Tags {
		where = append(where, fmt.Sprintf("%s_%s", k, v))
	}

	params := []interface{}{
		tq.ServiceName,
		tq.OperationName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		where,
		tq.NumTraces,
	}

	return cs.executeIDQuery(span, queryIDsByServiceAndOperationNameAndTags, params)
}

func (cs *couchbaseSpanReader) queryIDsByTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByTagsAndLogs", queryIDsByTag)
	defer span.Finish()

	var where []string
	for k, v := range tq.Tags {
		where = append(where, fmt.Sprintf("%s_%s", k, v))
	}

	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		where,
		tq.NumTraces,
	}

	return cs.executeIDQuery(span, queryIDsByTag, params)
}

func (cs *couchbaseSpanReader) queryIDsByDuration(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByDuration", queryIDsByDuration)
	defer span.Finish()

	minDuration := traceQuery.DurationMin.Nanoseconds()
	maxDuration := (time.Hour * 24).Nanoseconds()
	if traceQuery.DurationMax != 0 {
		maxDuration = traceQuery.DurationMax.Nanoseconds()
	}

	params := []interface{}{
		traceQuery.ServiceName,
		traceQuery.OperationName,
		minDuration,
		maxDuration,
		traceQuery.NumTraces,
	}

	return cs.executeIDQuery(span, queryIDsByDuration, params)
}

func (cs *couchbaseSpanReader) queryIDsByServiceNameAndOperation(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByServiceNameAndOperation", queryIDsByServiceAndOperationName)
	defer span.Finish()

	params := []interface{}{
		tq.ServiceName,
		tq.OperationName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeIDQuery(span, queryIDsByServiceAndOperationName, params)
}

func (cs *couchbaseSpanReader) queryIDsByService(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := cs.startSpanForQuery(ctx, "queryIDsByService", queryIDsByServiceName)
	defer span.Finish()

	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeIDQuery(span, queryIDsByServiceName, params)
}

func (cs *couchbaseSpanReader) executeIDQuery(span opentracing.Span, query string, params []interface{}) (UniqueTraceIDs, error) {
	// start := time.Now()
	var traceID TraceID
	traceIDs := make(UniqueTraceIDs)

	result, err := cs.store.Query(query, params)
	if err != nil {
		cs.logErrorToSpan(span, err)
		return nil, err
	}

	for result.Next(&traceID) {
		traceIDs.Add(traceID)
	}

	err = result.Close()
	if err != nil {
		cs.logErrorToSpan(span, err)
		return nil, err
	}

	return traceIDs, nil
}

func (cs *couchbaseSpanReader) validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}
	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}
	if !p.StartTimeMin.IsZero() && !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	if (p.DurationMin != 0 || p.DurationMax != 0) && len(p.Tags) > 0 {
		return ErrDurationAndTagQueryNotSupported
	}
	return nil
}

func (cs *couchbaseSpanReader) startSpanForQuery(ctx context.Context, name, query string) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, name)
	ottag.DBStatement.Set(span, query)
	ottag.DBType.Set(span, "couchbase")
	ottag.Component.Set(span, "analytics")
	return span, ctx
}

func (cs *couchbaseSpanReader) logErrorToSpan(span opentracing.Span, err error) {
	if err == nil {
		return
	}
	ottag.Error.Set(span, true)
	span.LogFields(otlog.Error(err))
}
