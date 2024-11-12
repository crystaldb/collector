package google_cloudsql

import (
	"context"
	"fmt"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/pganalyze/collector/state"
	"github.com/pganalyze/collector/util"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var newMetricClientFunc = monitoring.NewMetricClient

// GetSystemState - Gets system information about a Google Cloud SQL instance
func GetSystemState(ctx context.Context, server *state.Server, logger *util.Logger) (system state.SystemState) {
	logger.PrintInfo("CloudSQL/System: Called GetSystemState")

	config := server.Config
	system.Info.Type = state.GoogleCloudSQLSystem

	if config.GcpCredentialsFile == "" && config.GcpProjectID == "" {
		server.SelfTest.MarkCollectionAspectError(state.CollectionAspectSystemStats, "missing GCP credentials or project ID")
		logger.PrintError("CloudSQL/System: Missing GCP credentials or project ID")
		return
	}

	var opts []option.ClientOption
	if config.GcpCredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.GcpCredentialsFile))
	}

	client, err := newMetricClientFunc(ctx, opts...)
	if err != nil {
		server.SelfTest.MarkCollectionAspectError(state.CollectionAspectSystemStats, "error creating monitoring client: %v", err)
		logger.PrintError("CloudSQL/System: Failed to create monitoring client: %v", err)
		return
	}
	defer client.Close()

	instanceID := config.GcpCloudSQLInstanceID
	filter := fmt.Sprintf(
		`resource.type = "cloudsql_database" AND resource.labels.database_id = "%s:%s"`,
		config.GcpProjectID,
		instanceID,
	)

	// Create base request template
	baseReq := &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", config.GcpProjectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			EndTime:   &timestamppb.Timestamp{Seconds: time.Now().Unix()},
			StartTime: &timestamppb.Timestamp{Seconds: time.Now().Add(-5 * time.Minute).Unix()},
		},
	}

	// Memory stats
	system.Memory.TotalBytes = uint64(getMemoryMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/quota"))
	usedMemory := uint64(getMemoryMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/usage"))
	system.Memory.FreeBytes = system.Memory.TotalBytes - usedMemory
	totalUsageBytes := uint64(getMemoryMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/total_usage"))
	bufferAndCacheBytes := totalUsageBytes - usedMemory
	// TODO: currently, there is no way to separate the cache and buffer usage via CloudSQL metrics
	// so we just use the total buffer and cache usage as the cached bytes
	system.Memory.CachedBytes = bufferAndCacheBytes
	system.Memory.SwapUsedBytes = uint64(getMemoryMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/swap/bytes_used"))

	// CPU Stats with more detailed metrics
	system.CPUStats = make(state.CPUStatisticMap)
	system.CPUStats["all"] = state.CPUStatistic{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedSystemCPUStats{
			UserPercent: getCPUMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/cpu/utilization"),
		},
	}

	// Disk stats with I/O metrics
	system.DiskStats = make(state.DiskStatsMap)
	system.DiskStats["default"] = state.DiskStats{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedDiskStats{
			ReadOperationsPerSecond:  float64(getDiskMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/read_ops_count")),
			WriteOperationsPerSecond: float64(getDiskMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/write_ops_count")),
			UtilizationPercent:       float64(getDiskMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/utilization")),
		},
	}

	system.DiskPartitions = make(state.DiskPartitionMap)
	system.DiskPartitions["/"] = state.DiskPartition{
		DiskName:      "cloudsql",
		PartitionName: "cloudsql",
		UsedBytes:     uint64(getDiskMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/bytes_used")),
		TotalBytes:    uint64(getDiskMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/quota")),
	}

	// Network stats
	system.NetworkStats = make(state.NetworkStatsMap)
	system.NetworkStats["default"] = state.NetworkStats{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedNetworkStats{
			ReceiveThroughputBytesPerSecond:  uint64(getNetworkMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/network/received_bytes_count")),
			TransmitThroughputBytesPerSecond: uint64(getNetworkMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/network/sent_bytes_count")),
		},
	}

	server.SelfTest.MarkCollectionAspectOk(state.CollectionAspectSystemStats)

	return
}

func cloneRequest(req *monitoringpb.ListTimeSeriesRequest) *monitoringpb.ListTimeSeriesRequest {
	return &monitoringpb.ListTimeSeriesRequest{
		Name:     req.Name,
		Filter:   req.Filter,
		Interval: req.Interval,
	}
}

func getCPUMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) float64 {
	req.Filter = fmt.Sprintf("%s AND metric.type = %q", req.Filter, metricType)

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()
	if err == iterator.Done {
		return 0
	}
	if err != nil {
		return 0
	}

	if len(ts.Points) > 0 {
		value := ts.Points[0].Value.GetDoubleValue() * 100
		return value
	}

	return 0
}

func getMemoryMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) int64 {
	req.Filter = fmt.Sprintf("%s AND metric.type = %q", req.Filter, metricType)

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()
	if err == iterator.Done {
		return 0
	}
	if err != nil {
		return 0
	}

	if len(ts.Points) > 0 {
		value := ts.Points[0].Value.GetInt64Value()
		return value
	}

	return 0
}

func getNetworkMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) float64 {

	req.Filter = fmt.Sprintf("%s AND metric.type = %q", req.Filter, metricType)

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()
	if err == iterator.Done {
		return 0
	}
	if err != nil {
		return 0
	}

	if len(ts.Points) > 0 {
		value := ts.Points[0].Value.GetDoubleValue()
		return value
	}

	return 0
}

func getDiskMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) int64 {
	req.Filter = fmt.Sprintf("%s AND metric.type = %q", req.Filter, metricType)

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()
	if err == iterator.Done {
		return 0
	}
	if err != nil {
		return 0
	}

	if len(ts.Points) > 0 {
		value := ts.Points[0].Value.GetInt64Value()
		return value
	}

	return 0
}
