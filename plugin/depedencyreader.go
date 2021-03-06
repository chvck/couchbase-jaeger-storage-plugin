package plugin

import (
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/pkg/errors"
)

var (
	depsSelectStmt = "SELECT ts, dependencies FROM %s WHERE ts >= ? AND ts < ?"
)

type Dependency struct {
	Deps []model.DependencyLink `json:"dependencies"`
	Ts   time.Time              `json:"ts"`
}

type couchbaseDependencyReader struct {
	store Store
}

func (cs *couchbaseDependencyReader) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	result, err := cs.store.Query(
		depsSelectStmt,
		[]interface{}{endTs.Add(-1 * lookback).Format(dateLayout), endTs.Format(dateLayout)},
	)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading dependencies from storage")
	}

	var deps []model.DependencyLink
	var resDep Dependency
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
