package intel

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"time"
)

// Core is the common component of all generators for all systems.
type Core struct {
	*common.Core
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err
}
