package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

type HourlyAvgMetricsForClusters struct {
	core     utils.QueryGenerator
	clusters int
	metrics  int
	duration time.Duration
}

func NewHourlyAvgMetricsForClusters(metrics int, clusters int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &HourlyAvgMetricsForClusters{
			core:     core,
			clusters: clusters,
			metrics:  metrics,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *HourlyAvgMetricsForClusters) Fill(q query.Query) query.Query {
	fc, ok := d.core.(HourlyAvgMetricsForClustersFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.HourlyAvgMetricsForClusters(q, d.metrics, d.clusters, d.duration)
	return q
}
