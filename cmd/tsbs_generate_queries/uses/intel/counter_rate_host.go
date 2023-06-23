package intel

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type CounterRateHost struct {
	core     utils.QueryGenerator
	hosts    int
	duration time.Duration
}

func NewCounterRateHost(hosts int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &CounterRateHost{
			core:     core,
			hosts:    hosts,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *CounterRateHost) Fill(q query.Query) query.Query {
	fc, ok := d.core.(CounterRateHostFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.CounterRateHost(q, d.hosts, d.duration)
	return q
}
