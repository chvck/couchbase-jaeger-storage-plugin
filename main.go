package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"

	"gopkg.in/couchbase/gocb.v1"

	"github.com/hashicorp/go-plugin"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	ottag "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/spf13/viper"
)

var configPath string

var (
	querySpanByTraceID = `
SELECT trace_id, span_id, operation_name, flags, start_time, duration, tags, logs, references, process
FROM %s
WHERE trace_id.hi = ? AND trace_id.lo = ? AND ` + "`type`" + `="span"`
	queryServiceNames   = `SELECT DISTINCT process.service_name from %s where ` + "`type`" + `="span"`
	queryOperationNames = `SELECT DISTINCT operation_name from %s where process.service_name = ? AND ` + "`type`" + `="span"`
	queryIDsByTag       = `
SELECT DISTINCT b.trace_id
FROM %s AS b
WHERE b.process.service_name = ? AND b.start_time > ? AND b.start_time < ? AND ` + "b.`type`" + `="span" AND (EVERY tag IN ? SATISFIES tag IN b.processed_tags END)
ORDER BY b.start_time DESC
LIMIT ?`
	queryIDsByServiceName = `
SELECT DISTINCT trace_id
FROM %s
WHERE process.service_name = ? AND start_time > ? AND start_time < ? AND ` + "`type`" + `="span"
ORDER BY start_time DESC
LIMIT ?`
	queryIDsByServiceAndOperationName = `
SELECT DISTINCT trace_id
FROM %s AS b
WHERE process.service_name = ? AND operation_name = ? AND start_time > ? AND start_time < ? AND` + "`type`" + `="span"
ORDER BY start_time DESC
LIMIT ?`
	queryIDsByDuration = `
SELECT DISTINCT trace_id
FROM %s AS b
WHERE process.service_name = ? AND operation_name = ? AND duration > ? AND duration < ? AND ` + "`type`" + `="span"
LIMIT ?`
	depsSelectStmt = "SELECT ts, dependencies FROM %s WHERE ts >= ? AND ts < ?"
)

const (
	maximumTagKeyOrValueSize = 256
	defaultNumTraces         = 100
	dateLayout               = "2006-01-02T15:04:05.000Z"
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

type couchbaseStore struct {
	bucket *gocb.Bucket
}

type dependency struct {
	Deps []model.DependencyLink `json:"dependencies"`
	Ts   time.Time              `json:"ts"`
}

type Tag struct {
	Tag string `json:"tag"`
}

type TagInsertion struct {
	TagKey   string `json:"tag_key"`
	TagValue string `json:"tag_value"`
}

type UniqueTraceIDs map[TraceID]struct{}

var (
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

func main() {
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	v := viper.New()
	if configPath != "" {
		v.SetConfigFile(configPath)
	}

	v.SetDefault(bucketName, "default")
	v.SetDefault(connStr, "couchbase://localhost")

	if configPath != "" {
		err := v.ReadInConfig()
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}

	var options Options
	options.InitFromViper(v)

	gocb.SetLogger(gocb.VerboseStdioLogger())
	cluster, err := gocb.Connect(options.ConnStr)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	bucket, err := cluster.OpenBucket(options.BucketName, "")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	store := couchbaseStore{bucket: bucket}

	querySpanByTraceID = fmt.Sprintf(querySpanByTraceID, options.BucketName)
	queryServiceNames = fmt.Sprintf(queryServiceNames, options.BucketName)
	queryOperationNames = fmt.Sprintf(queryOperationNames, options.BucketName)
	queryIDsByTag = fmt.Sprintf(queryIDsByTag, options.BucketName)
	queryIDsByServiceName = fmt.Sprintf(queryIDsByServiceName, options.BucketName)
	queryIDsByServiceAndOperationName = fmt.Sprintf(queryIDsByServiceAndOperationName, options.BucketName)
	queryIDsByDuration = fmt.Sprintf(queryIDsByDuration, options.BucketName)
	depsSelectStmt = fmt.Sprintf(depsSelectStmt, options.BucketName)

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: plugin.HandshakeConfig{
			MagicCookieKey:   "STORAGE_PLUGIN",
			MagicCookieValue: "jaeger",
		},
		VersionedPlugins: map[int]plugin.PluginSet{
			1: map[string]plugin.Plugin{
				shared.StoragePluginIdentifier: &shared.StorageGRPCPlugin{
					Impl: &store,
				},
			},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}

func (cs *couchbaseStore) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	query := gocb.NewAnalyticsQuery(depsSelectStmt) // .AdHoc(false)
	result, err := cs.bucket.ExecuteAnalyticsQuery(
		query,
		[]interface{}{endTs.Add(-1 * lookback).Format(dateLayout), endTs.Format(dateLayout)},
	)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading dependencies from storage")
	}

	var deps []model.DependencyLink
	var resDep dependency
	for result.Next(&resDep) {
		for _, dep := range resDep.Deps {
			deps = append(deps, dep)
		}
	}

	if err = result.Close(); err != nil {
		return nil, errors.Wrap(err, "Error reading dependencies from storage")
	}

	return deps, nil
}

func (cs *couchbaseStore) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := startSpanForQuery(ctx, "readTrace", querySpanByTraceID)
	defer span.Finish()
	span.LogFields(otlog.String("event", "searching"), otlog.Object("trace_id", traceID))

	query := gocb.NewAnalyticsQuery(querySpanByTraceID)
	dbTraceID := traceIDFromDomain(traceID)
	result, err := cs.bucket.ExecuteAnalyticsQuery(query, []interface{}{dbTraceID.High, dbTraceID.Low})
	if err != nil {
		logErrorToSpan(span, err)
		return nil, err
	}

	var trace model.Trace
	var traceSpan Span
	for result.Next(&traceSpan) {
		startTime, err := time.Parse(dateLayout, traceSpan.StartTime)
		if err != nil {
			return nil, err
		}
		modelSpan := model.Span{
			TraceID:       traceIDToDomain(traceSpan.TraceID),
			SpanID:        model.SpanID(traceSpan.SpanID),
			Duration:      traceSpan.Duration,
			StartTime:     startTime,
			OperationName: traceSpan.OperationName,
			ProcessID:     traceSpan.ProcessID,
			Flags:         traceSpan.Flags,
			Logs:          traceSpan.Logs,
			Warnings:      traceSpan.Warnings,
			Tags:          traceSpan.Tags,
		}
		modelSpan.Process = &model.Process{
			ServiceName: traceSpan.Process.ServiceName,
			Tags:        traceSpan.Process.Tags,
		}
		for _, ref := range traceSpan.References {
			modelSpan.References = append(modelSpan.References, model.SpanRef{
				SpanID:  model.SpanID(ref.SpanID),
				TraceID: traceIDToDomain(ref.TraceID),
				RefType: model.SpanRefType(ref.RefType),
			})
		}

		trace.Spans = append(trace.Spans, &modelSpan)
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

func (cs *couchbaseStore) GetServices(ctx context.Context) ([]string, error) {
	query := gocb.NewAnalyticsQuery(queryServiceNames)
	result, err := cs.bucket.ExecuteAnalyticsQuery(query, nil)
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

func (cs *couchbaseStore) GetOperations(ctx context.Context, service string) ([]string, error) {
	query := gocb.NewAnalyticsQuery(queryOperationNames)
	result, err := cs.bucket.ExecuteAnalyticsQuery(query, []interface{}{service})
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

func (cs *couchbaseStore) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	uniqueTraceIDs, err := cs.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	var retMe []*model.Trace
	for _, traceID := range uniqueTraceIDs {
		trace, err := cs.GetTrace(ctx, traceID)
		if err != nil {
			// cs.logger.Error("Failure to read trace", zap.String("trace_id", traceID.String()), zap.Error(err))
			// panic("wja")
			continue
		}
		retMe = append(retMe, trace)
	}
	return retMe, nil
}

func (cs *couchbaseStore) FindTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	if err := validateQuery(traceQuery); err != nil {
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

func (cs *couchbaseStore) findTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	if traceQuery.DurationMin != 0 || traceQuery.DurationMax != 0 {
		return cs.queryByDuration(ctx, traceQuery)
	}

	if traceQuery.OperationName != "" {
		traceIds, err := cs.queryByServiceNameAndOperation(ctx, traceQuery)
		if err != nil {
			return nil, err
		}
		if len(traceQuery.Tags) > 0 {
			tagTraceIds, err := cs.queryByTagsAndLogs(ctx, traceQuery)
			if err != nil {
				return nil, err
			}
			return IntersectTraceIDs([]UniqueTraceIDs{
				traceIds,
				tagTraceIds,
			}), nil
		}
		return traceIds, nil
	}
	if len(traceQuery.Tags) > 0 {
		return cs.queryByTagsAndLogs(ctx, traceQuery)
	}
	return cs.queryByService(ctx, traceQuery)
}

func (cs *couchbaseStore) queryByTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByTagsAndLogs", queryIDsByTag)
	defer span.Finish()

	var where []string
	for k, v := range tq.Tags {
		where = append(where, fmt.Sprintf("%s_%s", k, v))
	}

	query := gocb.NewAnalyticsQuery(queryIDsByTag)
	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		where,
		tq.NumTraces,
	}

	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) queryByDuration(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByDuration", queryIDsByDuration)
	defer span.Finish()

	minDuration := traceQuery.DurationMin.Nanoseconds()
	maxDuration := (time.Hour * 24).Nanoseconds()
	if traceQuery.DurationMax != 0 {
		maxDuration = traceQuery.DurationMax.Nanoseconds()
	}

	query := gocb.NewAnalyticsQuery(queryIDsByDuration)
	params := []interface{}{
		traceQuery.ServiceName,
		traceQuery.OperationName,
		minDuration,
		maxDuration,
		traceQuery.NumTraces,
	}

	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) queryByServiceNameAndOperation(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByServiceNameAndOperation", queryIDsByServiceAndOperationName)
	defer span.Finish()
	query := gocb.NewAnalyticsQuery(queryIDsByServiceAndOperationName)
	params := []interface{}{
		tq.ServiceName,
		tq.OperationName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) queryByService(ctx context.Context, tq *spanstore.TraceQueryParameters) (UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByService", queryIDsByServiceName)
	defer span.Finish()
	query := gocb.NewAnalyticsQuery(queryIDsByServiceName)
	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) executeQuery(span opentracing.Span, query *gocb.AnalyticsQuery, params []interface{}) (UniqueTraceIDs, error) {
	// start := time.Now()
	var traceID struct {
		TraceID TraceID `json:"trace_id"`
	}
	traceIDs := make(UniqueTraceIDs)
	result, err := cs.bucket.ExecuteAnalyticsQuery(query, params)
	if err != nil {
		logErrorToSpan(span, err)
		return nil, err
	}

	for result.Next(&traceID) {
		traceIDs.Add(traceID.TraceID)
	}

	err = result.Close()
	if err != nil {
		logErrorToSpan(span, err)
		return nil, err
	}

	// tableMetrics.Emit(err, time.Since(start))
	// if err != nil {
	// 	logErrorToSpan(span, err)
	// 	// s.logger.Error("Failed to exec query", zap.Error(err))
	// 	return nil, err
	// }
	return traceIDs, nil
}

func (cs *couchbaseStore) WriteSpan(span *model.Span) error {
	dbSpan := Span{
		TraceID:       traceIDFromDomain(span.TraceID),
		SpanID:        uint64(span.SpanID),
		Duration:      span.Duration,
		StartTime:     span.StartTime.Format(dateLayout),
		OperationName: span.OperationName,
		ProcessID:     span.ProcessID,
		Flags:         span.Flags,
		Logs:          span.Logs,
		Process:       span.Process,
		Warnings:      span.Warnings,
		Tags:          span.Tags,
	}
	for _, ref := range span.References {
		dbSpan.References = append(dbSpan.References, SpanRef{
			SpanID:  uint64(ref.SpanID),
			TraceID: traceIDFromDomain(ref.TraceID),
			RefType: int32(ref.RefType),
		})
	}
	dbSpan.ProcessedTags = cs.getTags(span)

	dbSpan.Type = "span"
	_, err := cs.bucket.Upsert(fmt.Sprintf("%d", dbSpan.SpanID), dbSpan, 0)
	if err != nil {
		return err
	}

	return nil
}

func (cs *couchbaseStore) getTags(span *model.Span) []string {
	var tags []string
	for _, tag := range getAllUniqueTags(span) {
		if shouldIndexTag(tag) {
			tags = append(tags, tag.TagKey+"_"+tag.TagValue)
		} else {
			// s.tagIndexSkipped.Inc(1)
		}
	}

	return tags
}

//
// func (cs *couchbaseStore) writeTags(span *model.Span) error {
// 	for _, tag := range getAllUniqueTags(span) {
// 		if shouldIndexTag(tag) {
// 			dbTag := Tag{
// 				TraceID:   traceIDFromDomain(span.TraceID),
// 				SpanID:    uint64(span.SpanID),
// 				StartTime: span.StartTime,
// 				Type:      "tag",
// 			}
// 			dbTag.ServiceName = tag.ServiceName
// 			dbTag.TagKey = tag.TagKey
// 			dbTag.TagValue = tag.TagValue
// 			_, err := cs.bucket.Upsert(
// 				fmt.Sprintf("%d-%s-%s", dbTag.SpanID, dbTag.TagKey, dbTag.StartTime.String()),
// 				dbTag,
// 				0,
// 			)
// 			if err != nil {
// 				return err
// 			}
// 			// if err := s.writerMetrics.tagIndex.Exec(insertTagQuery, s.logger); err != nil {
// 			// 	withTagInfo := s.logger.
// 			// 		With(zap.String("tag_key", v.TagKey)).
// 			// 		With(zap.String("tag_value", v.TagValue)).
// 			// 		With(zap.String("service_name", v.ServiceName))
// 			// 	return s.logError(ds, err, "Failed to index tag", withTagInfo)
// 			// }
// 		} else {
// 			// s.tagIndexSkipped.Inc(1)
// 		}
// 	}
//
// 	return nil
// }

// shouldIndexTag checks to see if the tag is json or not, if it's UTF8 valid and it's not too large
func shouldIndexTag(tag TagInsertion) bool {
	isJSON := func(s string) bool {
		var js map[string]interface{}
		// poor man's string-is-a-json check shortcircuits full unmarshalling
		return strings.HasPrefix(s, "{") && json.Unmarshal([]byte(s), &js) == nil
	}

	return len(tag.TagKey) < maximumTagKeyOrValueSize &&
		len(tag.TagValue) < maximumTagKeyOrValueSize &&
		utf8.ValidString(tag.TagValue) &&
		utf8.ValidString(tag.TagKey) &&
		!isJSON(tag.TagValue)
}

func getAllUniqueTags(span *model.Span) []TagInsertion {
	allTags := append(model.KeyValues{}, span.Process.Tags...)
	allTags = append(allTags, span.Tags...)
	for _, log := range span.Logs {
		allTags = append(allTags, log.Fields...)
	}
	allTags.Sort()
	uniqueTags := make([]TagInsertion, 0, len(allTags))
	for i := range allTags {
		if allTags[i].VType == model.BinaryType {
			continue // do not index binary tags
		}
		if i > 0 && allTags[i-1].Equal(&allTags[i]) {
			continue // skip identical tags
		}
		uniqueTags = append(uniqueTags, TagInsertion{
			// ServiceName: span.Process.ServiceName,
			TagKey:   allTags[i].Key,
			TagValue: allTags[i].AsString(),
		})
	}
	return uniqueTags
}

func startSpanForQuery(ctx context.Context, name, query string) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, name)
	ottag.DBStatement.Set(span, query)
	ottag.DBType.Set(span, "couchbase")
	ottag.Component.Set(span, "analytics")
	return span, ctx
}

func logErrorToSpan(span opentracing.Span, err error) {
	if err == nil {
		return
	}
	ottag.Error.Set(span, true)
	span.LogFields(otlog.Error(err))
}

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

func validateQuery(p *spanstore.TraceQueryParameters) error {
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

// IntersectTraceIDs takes a list of UniqueTraceIDs and intersects them.
func IntersectTraceIDs(uniqueTraceIdsList []UniqueTraceIDs) UniqueTraceIDs {
	retMe := UniqueTraceIDs{}
	for key, value := range uniqueTraceIdsList[0] {
		keyExistsInAll := true
		for _, otherTraceIds := range uniqueTraceIdsList[1:] {
			if _, ok := otherTraceIds[key]; !ok {
				keyExistsInAll = false
				break
			}
		}
		if keyExistsInAll {
			retMe[key] = value
		}
	}
	return retMe
}

// Add adds a traceID to the existing map
func (u UniqueTraceIDs) Add(traceID TraceID) {
	u[traceID] = struct{}{}
}
