package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// LastPointForHosts returns QueryFiller for the intel lastpoint case
type LastPointForHosts struct {
	core  utils.QueryGenerator
	hosts int
}

// NewLastPointForHosts returns a new LastPointForHosts for given parameters
func NewLastPointForHosts(hosts int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &LastPointForHosts{
			core:  core,
			hosts: hosts,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *LastPointForHosts) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPointForHostsFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.LastPointForHosts(q, d.hosts)
	return q
}
