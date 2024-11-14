package google_cloudsql

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/pganalyze/collector/config"
	"github.com/pganalyze/collector/state"
	"github.com/pganalyze/collector/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIntegrationGetSystemState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// These environment variables need to be set for the integration test
	projectID := os.Getenv("GCP_PROJECT_ID")
	instanceID := os.Getenv("GCP_CLOUDSQL_INSTANCE_ID")
	credentialsFile := os.Getenv("GCP_CREDENTIALS_FILE")

	if projectID == "" || instanceID == "" || credentialsFile == "" {
		t.Skip("GCP credentials not set, skipping integration test")
	}

	ctx := context.Background()
	logger := &util.Logger{
		Verbose:     false,
		Quiet:       false,
		Destination: log.New(os.Stderr, "", log.LstdFlags),
	}

	server := &state.Server{
		Config: config.ServerConfig{
			SystemType:            "google_cloudsql",
			GcpProjectID:          projectID,
			GcpCloudSQLInstanceID: instanceID,
			GcpCredentialsFile:    credentialsFile,
		},
		SelfTest: state.MakeSelfTest(),
	}

	system := GetSystemState(ctx, server, logger)

	// Verify system type
	assert.Equal(t, state.GoogleCloudSQLSystem, system.Info.Type)

	// Verify CPU stats exist and are reasonable
	cpuStats, ok := system.CPUStats["all"]
	require.True(t, ok, "CPU stats should exist")
	assert.True(t, cpuStats.DiffedOnInput)
	assert.NotNil(t, cpuStats.DiffedValues)
	assert.True(t, cpuStats.DiffedValues.UserPercent >= 0 && cpuStats.DiffedValues.UserPercent <= 100,
		"CPU usage should be between 0 and 100 percent")

	// Verify memory stats are reasonable
	assert.True(t, system.Memory.TotalBytes > 0, "Total memory should be greater than 0")
	assert.True(t, system.Memory.FreeBytes >= 0, "Free memory should be non-negative")

	// Verify disk stats
	partition, ok := system.DiskPartitions["/"]
	require.True(t, ok, "Root partition should exist")
	assert.True(t, partition.TotalBytes > 0, "Total disk space should be greater than 0")
	assert.True(t, partition.UsedBytes > 0, "Used disk space should be greater than 0")
	assert.True(t, partition.UsedBytes <= partition.TotalBytes,
		"Used disk space should not exceed total space")

	// Verify self-test status
	assert.True(t, server.SelfTest.IsCollectionAspectOk(state.CollectionAspectSystemStats))
}

func TestRealWorldMetricValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	projectID := os.Getenv("GCP_PROJECT_ID")
	instanceID := os.Getenv("GCP_CLOUDSQL_INSTANCE_ID")
	credentialsFile := os.Getenv("GCP_CREDENTIALS_FILE")

	if projectID == "" || instanceID == "" || credentialsFile == "" {
		t.Skip("GCP credentials not set, skipping integration test")
	}

	server := &state.Server{
		Config: config.ServerConfig{
			SystemType:            "google_cloudsql",
			GcpProjectID:          projectID,
			GcpCloudSQLInstanceID: instanceID,
			GcpCredentialsFile:    credentialsFile,
		},
		SelfTest: state.MakeSelfTest(),
	}

	client, err := monitoring.NewMetricClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Test specific metrics with real values
	metrics := []string{
		"cloudsql.googleapis.com/database/memory/components",
		"cloudsql.googleapis.com/database/memory/quota",
		"cloudsql.googleapis.com/database/memory/total_usage",
		"cloudsql.googleapis.com/database/memory/usage",
		"cloudsql.googleapis.com/database/memory/utilization",
		"cloudsql.googleapis.com/database/swap/bytes_used",
		"cloudsql.googleapis.com/database/cpu/utilization",
		"cloudsql.googleapis.com/database/cpu/reserved_cores",
		"cloudsql.googleapis.com/database/cpu/usage_time",
		"cloudsql.googleapis.com/database/disk/bytes_used",
		"cloudsql.googleapis.com/database/disk/quota",
		"cloudsql.googleapis.com/database/disk/read_ops_count",
		"cloudsql.googleapis.com/database/disk/utilization",
		"cloudsql.googleapis.com/database/disk/write_ops_count",
		"cloudsql.googleapis.com/database/network/received_bytes_count",
		"cloudsql.googleapis.com/database/network/sent_bytes_count",
	}

	for _, metricType := range metrics {
		t.Run(metricType, func(t *testing.T) {
			value := getMetricValue(ctx, client, server.Config, metricType)
			assert.True(t, value >= 0, "Metric %s should have a positive value", metricType)
			t.Logf("Metric %s value: %v", metricType, value)
		})
	}
}

// Helper function to get a specific metric value
func getMetricValue(ctx context.Context, client *monitoring.MetricClient, config config.ServerConfig, metricType string) float64 {
	filter := fmt.Sprintf(
		`resource.type = "cloudsql_database" AND 
		 resource.labels.database_id = "%s:%s" AND
		 metric.type = "%s"`,
		config.GcpProjectID,
		config.GcpCloudSQLInstanceID,
		metricType,
	)

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", config.GcpProjectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			EndTime:   &timestamppb.Timestamp{Seconds: time.Now().Unix()},
			StartTime: &timestamppb.Timestamp{Seconds: time.Now().Add(-5 * time.Minute).Unix()},
		},
	}

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()

	if err != nil {
		log.Printf("Error getting time series: %v", err)
		return -1
	}

	if len(ts.Points) == 0 {
		log.Printf("No points found for metric %s", metricType)
		return -2
	}

	var result float64
	switch v := ts.Points[0].Value.Value.(type) {
	case *monitoringpb.TypedValue_DoubleValue:
		result = v.DoubleValue
	case *monitoringpb.TypedValue_Int64Value:
		result = float64(v.Int64Value)
	default:
		log.Printf("Unexpected value type: %T", v)
		return -3
	}

	return result
}
