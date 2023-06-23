package intel

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	errNoMetrics      = "cannot get 0 metrics"
	errTooManyMetrics = "too many metrics asked for"

	// LabelAllMetricsForHosts is the label prefix for queries
	LabelAllMetricsForHosts      = "all-metrics-host"
	LabelAllMetricsForCluster    = "all-metrics-cluster"
	LabelLastPointPrimary        = "last-point-primary-host"
	LabelLastPointForHosts       = "last-point-host"
	LabelTopKHostsForCluster     = "topk-host"
	LabelTopKPrimariesForCluster = "topk-primary"
	LabelAvgMetricsForHosts      = "avg-metrics-host"
	LabelClusterDailyAverage     = "cluster-daily-average"
	LabelCounterRateHost         = "counter-rate-host"
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

// 100 clusters of 30 shards, 3 hosts each
// 100 * 30 * 3 = 9000

// GetRandomHosts returns a random set of nHosts from a given Core
func (d *Core) GetRandomHosts(nHosts int) ([]string, error) {
	return getRandomNameOfMetadataType(nHosts, 9000, Host)
}

// GetRandomClusters returns a random set of nClusters from a given Core
func (d *Core) GetRandomClusters(nClusters int) ([]string, error) {
	return getRandomNameOfMetadataType(nClusters, 100, Cluster)
}

// GetRandomReplicas returns a random set of nReplicas from a given Core
func (d *Core) GetRandomReplicas(nReplicas int) ([]string, error) {
	return getRandomNameOfMetadataType(nReplicas, 30, Replica)
}

// GetRandomGroups returns a random set of nGroups from a given Core
func (d *Core) GetRandomGroups(nGroups int) ([]string, error) {
	return getRandomNameOfMetadataType(nGroups, 100, Group)
}

// GetRandomOrgs returns a random set of nOrgs from a given Core
func (d *Core) GetRandomOrgs(nOrgs int) ([]string, error) {
	return getRandomNameOfMetadataType(nOrgs, 100, Org)
}

type AllMetricsForHostsFiller interface {
	AllMetricsForHosts(query.Query, int, time.Duration)
}

type AllMetricsForClustersFiller interface {
	AllMetricsForClusters(query.Query, int, time.Duration)
}

type HourlyAvgMetricsForHostsFiller interface {
	HourlyAvgMetricsForHosts(query.Query, int, int, time.Duration)
}

type CounterRateHostFiller interface {
	CounterRateHost(query.Query, int, time.Duration)
}

type HourlyAvgMetricsForClustersFiller interface {
	HourlyAvgMetricsForClusters(query.Query, int, int, time.Duration)
}

type TopKHostsFromClusterFiller interface {
	TopKHostsFromCluster(query.Query, int, time.Duration)
}

type TopKPrimariesFromClusterFiller interface {
	TopKPrimariesFromCluster(query.Query, int, time.Duration)
}

type LastPointFiller interface {
	LastPointPrimary(query.Query)
}

type LastPointForHostsFiller interface {
	LastPointForHosts(query.Query, int)
}

type MetadataType string

const (
	Host    MetadataType = "host"
	Cluster              = "cluster"
	Replica              = "replica"
	Group                = "group"
	Org                  = "org"
)

// getRandomNameOfMetadataType returns a subset of num names of a permutation of names of a particular type,
// numbered from 0 to total.
// Ex.: if type is host, host_12, host_7, host_25 for numH=3 and total=30 (3 out of 30)
func getRandomNameOfMetadataType(num int, total int, metadataType MetadataType) ([]string, error) {
	if num < 1 {
		return nil, fmt.Errorf("num cannot be < 1; got %d", num)
	}
	if num > total {
		return nil, fmt.Errorf("num (%d) larger than total. See --scale (%d)", num, total)
	}

	randomNumbers, err := common.GetRandomSubsetPerm(num, total)
	if err != nil {
		return nil, err
	}

	names := []string{}
	for _, n := range randomNumbers {
		names = append(names, fmt.Sprintf("%v_%d", metadataType, n))
	}

	return names, nil
}

// intelMetrics is the list of metric names for intel
var intelMetrics = []string{
	"mongodb_asserts_regular",
	"mongodb_asserts_rollovers",
	"mongodb_asserts_user",
	"mongodb_asserts_warning",
	"mongodb_clusterTime_signature_keyId",
	"mongodb_connections_active",
	"mongodb_connections_available",
	"mongodb_connections_awaitingTopologyChanges",
	"mongodb_connections_current",
	"mongodb_connections_exhaustHello",
	"mongodb_connections_exhaustIsMaster",
	"mongodb_connections_totalCreated",
	"mongodb_defaultRWConcern_localUpdateWallClockTime",
	"mongodb_electionMetrics_averageCatchUpOps",
	"mongodb_electionMetrics_catchUpTakeover_called",
	"mongodb_electionMetrics_catchUpTakeover_successful",
	"mongodb_electionMetrics_electionTimeout_called",
	"mongodb_electionMetrics_electionTimeout_successful",
	"mongodb_electionMetrics_freezeTimeout_called",
	"mongodb_electionMetrics_freezeTimeout_successful",
	"mongodb_electionMetrics_numCatchUps",
	"mongodb_electionMetrics_numCatchUpsAlreadyCaughtUp",
	"mongodb_electionMetrics_numCatchUpsFailedWithError",
	"mongodb_electionMetrics_numCatchUpsFailedWithNewTerm",
	"mongodb_electionMetrics_numCatchUpsFailedWithReplSetAbortPrimaryCatchUpCmd",
	"mongodb_electionMetrics_numCatchUpsSkipped",
	"mongodb_electionMetrics_numCatchUpsSucceeded",
	"mongodb_electionMetrics_numCatchUpsTimedOut",
	"mongodb_electionMetrics_numStepDownsCausedByHigherTerm",
	"mongodb_electionMetrics_priorityTakeover_called",
	"mongodb_electionMetrics_priorityTakeover_successful",
	"mongodb_electionMetrics_stepUpCmd_called",
	"mongodb_electionMetrics_stepUpCmd_successful",
	"mongodb_encryptionAtRest_encryptionEnabled",
	"mongodb_extra_info_input_blocks",
	"mongodb_extra_info_involuntary_context_switches",
	"mongodb_extra_info_maximum_resident_set_kb",
	"mongodb_extra_info_output_blocks",
	"mongodb_extra_info_page_faults",
	"mongodb_extra_info_page_reclaims",
	"mongodb_extra_info_system_time_us",
	"mongodb_extra_info_user_time_us",
	"mongodb_extra_info_voluntary_context_switches",
	"mongodb_flowControl_enabled",
	"mongodb_flowControl_isLagged",
	"mongodb_flowControl_isLaggedCount",
	"mongodb_flowControl_isLaggedTimeMicros",
	"mongodb_flowControl_locksPerKiloOp",
	"mongodb_flowControl_sustainerRate",
	"mongodb_flowControl_targetRateLimit",
	"mongodb_flowControl_timeAcquiringMicros",
	"mongodb_globalLock_activeClients_readers",
	"mongodb_globalLock_activeClients_total",
	"mongodb_globalLock_activeClients_writers",
	"mongodb_globalLock_currentQueue_readers",
	"mongodb_globalLock_currentQueue_total",
	"mongodb_globalLock_currentQueue_writers",
	"mongodb_globalLock_totalTime",
	"mongodb_localTime",
	"mongodb_locks_Collection_acquireCount_W",
	"mongodb_locks_Collection_acquireCount_r",
	"mongodb_locks_Collection_acquireCount_w",
	"mongodb_locks_Database_acquireCount_W",
	"mongodb_locks_Database_acquireCount_r",
	"mongodb_locks_Database_acquireCount_w",
	"mongodb_locks_Global_acquireCount_W",
	"mongodb_locks_Global_acquireCount_r",
	"mongodb_locks_Global_acquireCount_w",
	"mongodb_locks_Mutex_acquireCount_r",
	"mongodb_locks_ParallelBatchWriterMode_acquireCount_W",
	"mongodb_locks_ParallelBatchWriterMode_acquireCount_r",
	"mongodb_locks_ReplicationStateTransition_acquireCount_W",
	"mongodb_locks_ReplicationStateTransition_acquireCount_w",
	"mongodb_locks_ReplicationStateTransition_acquireWaitCount_w",
	"mongodb_locks_ReplicationStateTransition_timeAcquiringMicros_w",
	"mongodb_locks_oplog_acquireCount_W",
	"mongodb_locks_oplog_acquireCount_r",
	"mongodb_locks_oplog_acquireCount_w",
	"mongodb_logicalSessionRecordCache_activeSessionsCount",
	"mongodb_logicalSessionRecordCache_lastSessionsCollectionJobCursorsClosed",
	"mongodb_logicalSessionRecordCache_lastSessionsCollectionJobDurationMillis",
	"mongodb_logicalSessionRecordCache_lastSessionsCollectionJobEntriesEnded",
	"mongodb_logicalSessionRecordCache_lastSessionsCollectionJobEntriesRefreshed",
	"mongodb_logicalSessionRecordCache_lastSessionsCollectionJobTimestamp",
	"mongodb_logicalSessionRecordCache_lastTransactionReaperJobDurationMillis",
	"mongodb_logicalSessionRecordCache_lastTransactionReaperJobEntriesCleanedUp",
	"mongodb_logicalSessionRecordCache_lastTransactionReaperJobTimestamp",
	"mongodb_logicalSessionRecordCache_sessionCatalogSize",
	"mongodb_logicalSessionRecordCache_sessionsCollectionJobCount",
	"mongodb_logicalSessionRecordCache_transactionReaperJobCount",
	"mongodb_mem_bits",
	"mongodb_mem_resident",
	"mongodb_mem_supported",
	"mongodb_mem_virtual",
	"mongodb_members_health",
	"mongodb_members_id",
	"mongodb_members_optimeDate",
	"mongodb_members_optime_t",
	"mongodb_members_self",
	"mongodb_members_state",
	"mongodb_metrics_aggStageCounters_addFields",
	"mongodb_metrics_aggStageCounters_backupCursor",
	"mongodb_metrics_aggStageCounters_backupCursorExtend",
	"mongodb_metrics_aggStageCounters_bucket",
	"mongodb_metrics_aggStageCounters_bucketAuto",
	"mongodb_metrics_aggStageCounters_changeStream",
	"mongodb_metrics_aggStageCounters_collStats",
	"mongodb_metrics_aggStageCounters_count",
	"mongodb_metrics_aggStageCounters_currentOp",
	"mongodb_metrics_aggStageCounters_documents",
	"mongodb_metrics_aggStageCounters_facet",
	"mongodb_metrics_aggStageCounters_geoNear",
	"mongodb_metrics_aggStageCounters_graphLookup",
	"mongodb_metrics_aggStageCounters_group",
	"mongodb_metrics_aggStageCounters_indexStats",
	"mongodb_metrics_aggStageCounters_internalInhibitOptimization",
	"mongodb_metrics_aggStageCounters_internalSearchIdLookup",
	"mongodb_metrics_aggStageCounters_internalSearchMongotRemote",
	"mongodb_metrics_aggStageCounters_internalSplitPipeline",
	"mongodb_metrics_aggStageCounters_limit",
	"mongodb_metrics_aggStageCounters_listLocalSessions",
	"mongodb_metrics_aggStageCounters_listSessions",
	"mongodb_metrics_aggStageCounters_lookup",
	"mongodb_metrics_aggStageCounters_match",
	"mongodb_metrics_aggStageCounters_merge",
	"mongodb_metrics_aggStageCounters_mergeCursors",
	"mongodb_metrics_aggStageCounters_out",
	"mongodb_metrics_aggStageCounters_planCacheStats",
	"mongodb_metrics_aggStageCounters_project",
	"mongodb_metrics_aggStageCounters_queue",
	"mongodb_metrics_aggStageCounters_redact",
	"mongodb_metrics_aggStageCounters_replaceRoot",
	"mongodb_metrics_aggStageCounters_replaceWith",
	"mongodb_metrics_aggStageCounters_sample",
	"mongodb_metrics_aggStageCounters_search",
	"mongodb_metrics_aggStageCounters_searchBeta",
	"mongodb_metrics_aggStageCounters_searchMeta",
	"mongodb_metrics_aggStageCounters_set",
	"mongodb_metrics_aggStageCounters_skip",
	"mongodb_metrics_aggStageCounters_sort",
	"mongodb_metrics_aggStageCounters_sortByCount",
	"mongodb_metrics_aggStageCounters_unionWith",
	"mongodb_metrics_aggStageCounters_unset",
	"mongodb_metrics_aggStageCounters_unwind",
	"mongodb_metrics_commands_UNKNOWN",
	"mongodb_metrics_commands_abortTransaction_failed",
	"mongodb_metrics_commands_abortTransaction_total",
	"mongodb_metrics_commands_addShard_failed",
	"mongodb_metrics_commands_addShard_total",
	"mongodb_metrics_commands_aggregate_failed",
	"mongodb_metrics_commands_aggregate_total",
	"mongodb_metrics_commands_appendOplogNote_failed",
	"mongodb_metrics_commands_appendOplogNote_total",
	"mongodb_metrics_commands_applyOps_failed",
	"mongodb_metrics_commands_applyOps_total",
	"mongodb_metrics_commands_authenticate_failed",
	"mongodb_metrics_commands_authenticate_total",
	"mongodb_metrics_commands_availableQueryOptions_failed",
	"mongodb_metrics_commands_availableQueryOptions_total",
	"mongodb_metrics_commands_buildInfo_failed",
	"mongodb_metrics_commands_buildInfo_total",
	"mongodb_metrics_commands_checkShardingIndex_failed",
	"mongodb_metrics_commands_checkShardingIndex_total",
	"mongodb_metrics_commands_cleanupOrphaned_failed",
	"mongodb_metrics_commands_cleanupOrphaned_total",
	"mongodb_metrics_commands_cloneCollectionAsCapped_failed",
	"mongodb_metrics_commands_cloneCollectionAsCapped_total",
	"mongodb_metrics_commands_cloneCollectionOptionsFromPrimaryShard_failed",
	"mongodb_metrics_commands_cloneCollectionOptionsFromPrimaryShard_total",
	"mongodb_metrics_commands_collMod_failed",
	"mongodb_metrics_commands_collMod_total",
	"mongodb_metrics_commands_collStats_failed",
	"mongodb_metrics_commands_collStats_total",
	"mongodb_metrics_commands_commitTransaction_failed",
	"mongodb_metrics_commands_commitTransaction_total",
	"mongodb_metrics_commands_compact_failed",
	"mongodb_metrics_commands_compact_total",
	"mongodb_metrics_commands_configsvrAddShardToZone_failed",
	"mongodb_metrics_commands_configsvrAddShardToZone_total",
	"mongodb_metrics_commands_configsvrAddShard_failed",
	"mongodb_metrics_commands_configsvrAddShard_total",
	"mongodb_metrics_commands_configsvrBalancerCollectionStatus_failed",
	"mongodb_metrics_commands_configsvrBalancerCollectionStatus_total",
	"mongodb_metrics_commands_configsvrBalancerStart_failed",
	"mongodb_metrics_commands_configsvrBalancerStart_total",
	"mongodb_metrics_commands_configsvrBalancerStatus_failed",
	"mongodb_metrics_commands_configsvrBalancerStatus_total",
	"mongodb_metrics_commands_configsvrBalancerStop_failed",
	"mongodb_metrics_commands_configsvrBalancerStop_total",
	"mongodb_metrics_commands_configsvrClearJumboFlag_failed",
	"mongodb_metrics_commands_configsvrClearJumboFlag_total",
	"mongodb_metrics_commands_configsvrCommitChunkMerge_failed",
	"mongodb_metrics_commands_configsvrCommitChunkMerge_total",
	"mongodb_metrics_commands_configsvrCommitChunkMigration_failed",
	"mongodb_metrics_commands_configsvrCommitChunkMigration_total",
	"mongodb_metrics_commands_configsvrCommitChunkSplit_failed",
	"mongodb_metrics_commands_configsvrCommitChunkSplit_total",
	"mongodb_metrics_commands_configsvrCommitChunksMerge_failed",
	"mongodb_metrics_commands_configsvrCommitChunksMerge_total",
	"mongodb_metrics_commands_configsvrCommitMovePrimary_failed",
	"mongodb_metrics_commands_configsvrCommitMovePrimary_total",
	"mongodb_metrics_commands_configsvrCreateCollection_failed",
	"mongodb_metrics_commands_configsvrCreateCollection_total",
	"mongodb_metrics_commands_configsvrCreateDatabase_failed",
	"mongodb_metrics_commands_configsvrCreateDatabase_total",
	"mongodb_metrics_commands_configsvrDropCollection_failed",
	"mongodb_metrics_commands_configsvrDropCollection_total",
	"mongodb_metrics_commands_configsvrDropDatabase_failed",
	"mongodb_metrics_commands_configsvrDropDatabase_total",
	"mongodb_metrics_commands_configsvrEnableSharding_failed",
	"mongodb_metrics_commands_configsvrEnableSharding_total",
	"mongodb_metrics_commands_configsvrEnsureChunkVersionIsGreaterThan_failed",
	"mongodb_metrics_commands_configsvrEnsureChunkVersionIsGreaterThan_total",
	"mongodb_metrics_commands_configsvrMoveChunk_failed",
	"mongodb_metrics_commands_configsvrMoveChunk_total",
	"mongodb_metrics_commands_configsvrMovePrimary_failed",
	"mongodb_metrics_commands_configsvrMovePrimary_total",
	"mongodb_metrics_commands_configsvrRefineCollectionShardKey_failed",
	"mongodb_metrics_commands_configsvrRefineCollectionShardKey_total",
	"mongodb_metrics_commands_configsvrRemoveShardFromZone_failed",
	"mongodb_metrics_commands_configsvrRemoveShardFromZone_total",
	"mongodb_metrics_commands_configsvrRemoveShard_failed",
	"mongodb_metrics_commands_configsvrRemoveShard_total",
	"mongodb_metrics_commands_configsvrShardCollection_failed",
	"mongodb_metrics_commands_configsvrShardCollection_total",
	"mongodb_metrics_commands_configsvrUpdateZoneKeyRange_failed",
	"mongodb_metrics_commands_configsvrUpdateZoneKeyRange_total",
	"mongodb_metrics_commands_connPoolStats_failed",
	"mongodb_metrics_commands_connPoolStats_total",
	"mongodb_metrics_commands_connPoolSync_failed",
	"mongodb_metrics_commands_connPoolSync_total",
	"mongodb_metrics_commands_connectionStatus_failed",
	"mongodb_metrics_commands_connectionStatus_total",
	"mongodb_metrics_commands_convertToCapped_failed",
	"mongodb_metrics_commands_convertToCapped_total",
	"mongodb_metrics_commands_coordinateCommitTransaction_failed",
	"mongodb_metrics_commands_coordinateCommitTransaction_total",
	"mongodb_metrics_commands_count_failed",
	"mongodb_metrics_commands_count_total",
	"mongodb_metrics_commands_createIndexes_failed",
	"mongodb_metrics_commands_createIndexes_total",
	"mongodb_metrics_commands_createRole_failed",
	"mongodb_metrics_commands_createRole_total",
	"mongodb_metrics_commands_createUser_failed",
	"mongodb_metrics_commands_createUser_total",
	"mongodb_metrics_commands_create_failed",
	"mongodb_metrics_commands_create_total",
	"mongodb_metrics_commands_currentOp_failed",
	"mongodb_metrics_commands_currentOp_total",
	"mongodb_metrics_commands_dataSize_failed",
}

// GetIntelMetricsSlice returns a subset of metrics for the intel usecase
func GetIntelMetricsSlice(numMetrics int) ([]string, error) {
	if numMetrics <= 0 {
		return nil, fmt.Errorf(errNoMetrics)
	}
	if numMetrics > len(intelMetrics) {
		return nil, fmt.Errorf(errTooManyMetrics)
	}
	return intelMetrics[:numMetrics], nil
}

// GetAllIntelMetrics returns all the metrics in the intel usecase
func GetAllIntelMetrics() []string {
	return intelMetrics
}

// GetIntelMetricsLen returns the number of metrics in the intel usecase
func GetIntelMetricsLen() int {
	return len(intelMetrics)
}
