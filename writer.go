package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jaegertracing/jaeger/model"
)

const (
	maximumTagKeyOrValueSize = 256
	dateLayout               = "2006-01-02T15:04:05.000Z"
)

type couchbaseSpanWriter struct {
	store Store
}

func (cs *couchbaseSpanWriter) WriteSpan(span *model.Span) error {
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
	err := cs.store.Insert(fmt.Sprintf("%d", dbSpan.SpanID), dbSpan, 0)
	if err != nil {
		return err
	}

	return nil
}

func (cs *couchbaseSpanWriter) getTags(span *model.Span) []string {
	var tags []string
	for _, tag := range cs.getAllUniqueTags(span) {
		if cs.shouldStoreTag(tag) {
			tags = append(tags, tag.TagKey+"_"+tag.TagValue)
		}
	}

	return tags
}

// shouldStoreTag checks to see if the tag is json or not, if it's UTF8 valid and it's not too large
func (cs *couchbaseSpanWriter) shouldStoreTag(tag TagInsertion) bool {
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

func (cs *couchbaseSpanWriter) getAllUniqueTags(span *model.Span) []TagInsertion {
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
			TagKey:   allTags[i].Key,
			TagValue: allTags[i].AsString(),
		})
	}
	return uniqueTags
}
