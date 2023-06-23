package intel

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// AllMetricsPerCluster returns QueryFiller for the intel all metrics no rollup usecase
type AllMetricsPerCluster struct {
	core     utils.QueryGenerator
	clusters int
	duration time.Duration
}

// NewAllMetricsForClusters returns a new NewAllMetricsForClusters for given parameters
func NewAllMetricsForClusters(clusters int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &AllMetricsPerCluster{
			core:     core,
			clusters: clusters,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *AllMetricsPerCluster) Fill(q query.Query) query.Query {
	fc, ok := d.core.(AllMetricsForClustersFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.AllMetricsForClusters(q, d.clusters, d.duration)
	return q
}
