package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

// TopKPrimariesFromCluster returns QueryFiller for the intel all metrics no rollup usecase
type TopKPrimariesFromCluster struct {
	core     utils.QueryGenerator
	hosts    int
	duration time.Duration
}

// NewTopKPrimariesFromCluster returns a new TopKPrimaryFromCluster for given parameters
func NewTopKPrimariesFromCluster(hosts int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &TopKPrimariesFromCluster{
			core:     core,
			hosts:    hosts,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *TopKPrimariesFromCluster) Fill(q query.Query) query.Query {
	fc, ok := d.core.(TopKPrimariesFromClusterFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.TopKPrimariesFromCluster(q, d.hosts, d.duration)
	return q
}
