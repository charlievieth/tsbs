package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// LastPointPrimary returns QueryFiller for the intel lastpoint case
type LastPointPrimary struct {
	core utils.QueryGenerator
}

// NewLastPointPrimary returns a new LastPointPrimary for given parameters
func NewLastPointPrimary(core utils.QueryGenerator) utils.QueryFiller {
	return &LastPointPrimary{core}
}

// Fill fills in the query.Query with query details
func (d *LastPointPrimary) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPointFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.LastPointPrimary(q)
	return q
}
