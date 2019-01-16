package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jaegertracing/jaeger/plugin/storage/cassandra/spanstore/dbmodel"

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
SELECT trace_id, span_id, parent_id, operation_name, flags, start_time, duration, tags, logs, refs, process
FROM %s
WHERE trace_id = ? AND type="span"`
	queryServiceNames   = `SELECT DISTINCT process.service_name from %s where type="span"`
	queryOperationNames = `SELECT DISTINCT operation_name from %s where type="span"`
	queryByTag          = `
		SELECT DISTINCT trace_id
		FROM %s
		WHERE service_name = ? AND tag_key = ? AND tag_value = ? and start_time > ? and start_time < ? AND type="tag"
		ORDER BY start_time DESC
		LIMIT ?`
	queryByServiceName = `
		SELECT DISTINCT trace_id
		FROM %s
		WHERE process.service_name = ? AND start_time > ? AND start_time < ? AND type="span"
		ORDER BY start_time DESC
		LIMIT ?`
	queryByServiceAndOperationName = `
		SELECT DISTINCT trace_id
		FROM %s
		WHERE process.service_name = ? AND operation_name = ? AND start_time > ? AND start_time < ? AND type="span"
		ORDER BY start_time DESC
		LIMIT ?`
	queryByDuration = `
		SELECT DISTINCT trace_id
		FROM %s
		WHERE process.service_name = ? AND operation_name = ? AND duration > ? AND duration < ? AND type="span"
		LIMIT ?`
	depsSelectStmt = "SELECT ts, dependencies FROM %s WHERE ts >= ? AND ts < ?"
)

const (
	maximumTagKeyOrValueSize = 256
	defaultNumTraces         = 100
)

type Span struct {
	*model.Span
	TraceID [16]byte `json:"trace_id"`
	SpanID  uint64   `json:"span_id"`
	Type    string   `json:"type"`
}

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
	queryByTag = fmt.Sprintf(queryByTag, options.BucketName)
	queryByServiceName = fmt.Sprintf(queryByServiceName, options.BucketName)
	queryByServiceAndOperationName = fmt.Sprintf(queryByServiceAndOperationName, options.BucketName)
	queryByDuration = fmt.Sprintf(queryByDuration, options.BucketName)
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

type couchbaseStore struct {
	bucket *gocb.Bucket
}

type dependency struct {
	Deps []model.DependencyLink `json:"dependencies"`
	Ts   time.Time              `json:"ts"`
}

func (cs *couchbaseStore) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	query := gocb.NewN1qlQuery(depsSelectStmt).AdHoc(false)
	result, err := cs.bucket.ExecuteN1qlQuery(query, []interface{}{endTs.Add(-1 * lookback), endTs})
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

	query := gocb.NewN1qlQuery(querySpanByTraceID)
	result, err := cs.bucket.ExecuteN1qlQuery(query, []interface{}{traceIDFromDomain(traceID)})
	if err != nil {
		logErrorToSpan(span, err)
		return nil, err
	}

	var trace model.Trace
	var traceSpan Span
	for result.Next(&traceSpan) {
		traceSpan.Span.SpanID = model.SpanID(traceSpan.SpanID)
		traceSpan.Span.TraceID = traceIDToDomain(traceSpan.TraceID)
		trace.Spans = append(trace.Spans, traceSpan.Span)
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
	query := gocb.NewN1qlQuery(queryServiceNames)
	result, err := cs.bucket.ExecuteN1qlQuery(query, nil)
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
	query := gocb.NewN1qlQuery(queryOperationNames)
	result, err := cs.bucket.ExecuteN1qlQuery(query, nil)
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

func (cs *couchbaseStore) findTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (dbmodel.UniqueTraceIDs, error) {
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
			return IntersectTraceIDs([]dbmodel.UniqueTraceIDs{
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

func (cs *couchbaseStore) queryByTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) (dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByTagsAndLogs", queryByTag)
	defer span.Finish()

	results := make([]dbmodel.UniqueTraceIDs, 0, len(tq.Tags))
	for k, v := range tq.Tags {
		childSpan, _ := opentracing.StartSpanFromContext(ctx, "queryByTag")
		childSpan.LogFields(otlog.String("tag.key", k), otlog.String("tag.value", v))
		query := gocb.NewN1qlQuery(queryByTag)
		params := []interface{}{
			tq.ServiceName,
			k,
			v,
			tq.StartTimeMin,
			tq.StartTimeMax,
			tq.NumTraces,
		}

		t, err := cs.executeQuery(childSpan, query, params)
		childSpan.Finish()
		if err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	return IntersectTraceIDs(results), nil
}

func (cs *couchbaseStore) queryByDuration(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByDuration", queryByDuration)
	defer span.Finish()

	minDuration := traceQuery.DurationMin.Nanoseconds()
	maxDuration := (time.Hour * 24).Nanoseconds()
	if traceQuery.DurationMax != 0 {
		maxDuration = traceQuery.DurationMax.Nanoseconds()
	}

	query := gocb.NewN1qlQuery(queryByDuration)
	params := []interface{}{
		traceQuery.ServiceName,
		traceQuery.OperationName,
		minDuration,
		maxDuration,
		traceQuery.NumTraces,
	}

	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) queryByServiceNameAndOperation(ctx context.Context, tq *spanstore.TraceQueryParameters) (dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByServiceNameAndOperation", queryByServiceAndOperationName)
	defer span.Finish()
	query := gocb.NewN1qlQuery(queryByServiceAndOperationName)
	params := []interface{}{
		tq.ServiceName,
		tq.OperationName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) queryByService(ctx context.Context, tq *spanstore.TraceQueryParameters) (dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByService", queryByServiceName)
	defer span.Finish()
	query := gocb.NewN1qlQuery(queryByServiceName)
	params := []interface{}{
		tq.ServiceName,
		tq.StartTimeMin,
		tq.StartTimeMax,
		tq.NumTraces,
	}
	return cs.executeQuery(span, query, params)
}

func (cs *couchbaseStore) executeQuery(span opentracing.Span, query *gocb.N1qlQuery, params []interface{}) (dbmodel.UniqueTraceIDs, error) {
	// start := time.Now()
	var traceID struct {
		TraceID [16]byte `json:"trace_id"`
	}
	traceIDs := make(dbmodel.UniqueTraceIDs)
	result, err := cs.bucket.ExecuteN1qlQuery(query, params)
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
	dbSpan := Span{Span: span}
	dbSpan.TraceID = traceIDFromDomain(span.TraceID)
	dbSpan.SpanID = uint64(span.SpanID)
	dbSpan.Type = "span"
	_, err := cs.bucket.Upsert(fmt.Sprintf("%d", uint64(span.SpanID)), dbSpan, 0)
	if err != nil {
		return err
	}

	return cs.writeTags(span)
}

func (cs *couchbaseStore) writeTags(span *model.Span) error {
	for _, tag := range getAllUniqueTags(span) {
		if shouldIndexTag(tag) {
			dbTag := Tag{
				TraceID:   traceIDFromDomain(span.TraceID),
				SpanID:    uint64(span.SpanID),
				StartTime: span.StartTime,
				Type:      "tag",
			}
			dbTag.ServiceName = tag.ServiceName
			dbTag.TagKey = tag.TagKey
			dbTag.TagValue = tag.TagValue
			_, err := cs.bucket.Upsert(
				fmt.Sprintf("%d-%s-%s", dbTag.SpanID, dbTag.TagKey, dbTag.StartTime.String()),
				dbTag,
				0,
			)
			if err != nil {
				return err
			}
			// if err := s.writerMetrics.tagIndex.Exec(insertTagQuery, s.logger); err != nil {
			// 	withTagInfo := s.logger.
			// 		With(zap.String("tag_key", v.TagKey)).
			// 		With(zap.String("tag_value", v.TagValue)).
			// 		With(zap.String("service_name", v.ServiceName))
			// 	return s.logError(ds, err, "Failed to index tag", withTagInfo)
			// }
		} else {
			// s.tagIndexSkipped.Inc(1)
		}
	}

	return nil
}

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

type Tag struct {
	TagInsertion
	TraceID   [16]byte  `json:"trace_id"`
	SpanID    uint64    `json:"span_id"`
	StartTime time.Time `json:"start_time"`
	Type      string    `json:"type"`
}

type TagInsertion struct {
	ServiceName string `json:"service_name"`
	TagKey      string `json:"tag_key"`
	TagValue    string `json:"tag_value"`
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
			ServiceName: span.Process.ServiceName,
			TagKey:      allTags[i].Key,
			TagValue:    allTags[i].AsString(),
		})
	}
	return uniqueTags
}

func startSpanForQuery(ctx context.Context, name, query string) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, name)
	ottag.DBStatement.Set(span, query)
	ottag.DBType.Set(span, "couchbase")
	ottag.Component.Set(span, "n1ql")
	return span, ctx
}

func logErrorToSpan(span opentracing.Span, err error) {
	if err == nil {
		return
	}
	ottag.Error.Set(span, true)
	span.LogFields(otlog.Error(err))
}

func traceIDToDomain(traceID [16]byte) model.TraceID {
	var modelTraceID model.TraceID
	modelTraceID.High = binary.BigEndian.Uint64(traceID[:8])
	modelTraceID.Low = binary.BigEndian.Uint64(traceID[8:])
	return modelTraceID
}

func traceIDFromDomain(traceID model.TraceID) [16]byte {
	var dbTraceID [16]byte
	binary.BigEndian.PutUint64(dbTraceID[:8], uint64(traceID.High))
	binary.BigEndian.PutUint64(dbTraceID[8:], uint64(traceID.Low))
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
func IntersectTraceIDs(uniqueTraceIdsList []dbmodel.UniqueTraceIDs) dbmodel.UniqueTraceIDs {
	retMe := dbmodel.UniqueTraceIDs{}
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
