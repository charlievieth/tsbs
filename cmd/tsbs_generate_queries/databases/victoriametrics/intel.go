package victoriametrics

import "github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/intel"

// Intel produces ClickHouse-specific queries for all the Intel query types.
type Intel struct {
	*BaseGenerator
	*intel.Core
}

// mustGetRandomHosts is the form of GetRandomHosts that cannot error; if it does error,
// it causes a panic.
func (i *Intel) mustGetRandomHosts(nHosts int) []string {
	hosts, err := i.GetRandomHosts(nHosts)
	if err != nil {
		panic(err.Error())
	}
	return hosts
}
