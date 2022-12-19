package mongo

import "github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"

// Intel produces Mongo-specific queries for the Intel use case.
type Intel struct {
	*BaseGenerator
	*devops.Core
}
