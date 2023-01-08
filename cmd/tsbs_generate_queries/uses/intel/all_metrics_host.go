package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

// AllMetricsPerHost returns QueryFiller for the intel all metrics no rollup usecase
type AllMetricsPerHost struct {
	core     utils.QueryGenerator
	hosts    int
	duration time.Duration
}

// NewAllMetricsForHosts returns a new NewAllMetricsForHosts for given parameters
func NewAllMetricsForHosts(hosts int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &AllMetricsPerHost{
			core:     core,
			hosts:    hosts,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *AllMetricsPerHost) Fill(q query.Query) query.Query {
	fc, ok := d.core.(AllMetricsForHostsFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.AllMetricsForHosts(q, d.hosts, d.duration)
	return q
}
