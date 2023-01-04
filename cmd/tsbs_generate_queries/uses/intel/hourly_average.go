package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

// HourlyAvgMetricsForHosts returns QueryFiller for the intel all metrics no rollup usecase
type HourlyAvgMetricsForHosts struct {
	core     utils.QueryGenerator
	hosts    int
	metrics  int
	duration time.Duration
}

// NewHourlyAvgMetricsForHosts returns a new NewAllMetricsForHosts for given parameters
func NewHourlyAvgMetricsForHosts(metrics int, hosts int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &HourlyAvgMetricsForHosts{
			core:     core,
			hosts:    hosts,
			metrics:  metrics,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *HourlyAvgMetricsForHosts) Fill(q query.Query) query.Query {
	fc, ok := d.core.(HourlyAvgMetricsForHostsFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.HourlyAvgMetricsForHosts(q, d.metrics, d.hosts, d.duration)
	return q
}
