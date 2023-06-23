package intel

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// AllMetricsPerHost returns QueryFiller for the intel all metrics no rollup usecase
type TopKHostsFromCluster struct {
	core     utils.QueryGenerator
	hosts    int
	duration time.Duration
}

// NewTopKHostsFromCluster returns a new TopKHostsFromCluster for given parameters
func NewTopKHostsFromCluster(hosts int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &TopKHostsFromCluster{
			core:     core,
			hosts:    hosts,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *TopKHostsFromCluster) Fill(q query.Query) query.Query {
	fc, ok := d.core.(TopKHostsFromClusterFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.TopKHostsFromCluster(q, d.hosts, d.duration)
	return q
}
