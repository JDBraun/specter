package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"specter/internal"
)

var monitorCmd = &cobra.Command{
	Use:   "monitor",
	Short: "Monitor with pre-built queries.",
	Long:  "Run pre-defined monitoring queries to detect potentially suspicious activity in the Databricks audit logs.",
	Run: func(cmd *cobra.Command, args []string) {
		queryIndex, _ := cmd.Flags().GetInt("run")
		listQueries, _ := cmd.Flags().GetBool("list")

		if listQueries {
			listPredefinedQueries()
			resetMonitorFlags(cmd)
			return
		}

		if queryIndex < 0 || queryIndex > len(predefinedQueries) {
			log.Fatalf("Invalid query index. Please choose a valid number or use '--list' to see available queries.")
		}

		runQueries(queryIndex)
		resetMonitorFlags(cmd)
	},
}

var predefinedQueries = []struct {
	Name        string
	Description string
	Query       string
}{
	{
		Name:        "repeated_unauthorized_uc_data_requests",
		Description: "Detect repeated unauthorized UC data requests.",
		Query:       "WITH failed_data_access AS (SELECT date_trunc('hour', event_time) AS window_start, date_trunc('hour', event_time) + INTERVAL '1' HOUR AS window_end, user_identity.email, json_extract(request_params, '$.metastore_id') AS metastore_id, IF(json_extract(request_params, '$.workspace_id') IS NOT NULL, json_extract(request_params, '$.workspace_id'), workspace_id) AS workspace_id, action_name, CASE WHEN json_extract(request_params, '$.table_full_name') IS NOT NULL THEN json_extract(request_params, '$.table_full_name') WHEN json_extract(request_params, '$.volume_full_name') IS NOT NULL THEN json_extract(request_params, '$.volume_full_name') WHEN json_extract(request_params, '$.name') IS NOT NULL THEN json_extract(request_params, '$.name') WHEN json_extract(request_params, '$.url') IS NOT NULL THEN json_extract(request_params, '$.url') WHEN json_extract(request_params, '$.table_url') IS NOT NULL THEN json_extract(request_params, '$.table_url') WHEN json_extract(request_params, '$.table_id') IS NOT NULL THEN json_extract(request_params, '$.table_id') WHEN json_extract(request_params, '$.volume_id') IS NOT NULL THEN json_extract(request_params, '$.volume_id') ELSE NULL END AS securable, response.error_message FROM read_parquet(S3_PRESIGNED_URL) WHERE action_name IN ('generateTemporaryTableCredential', 'generateTemporaryPathCredential', 'generateTemporaryVolumeCredential', 'deltaSharingQueryTable', 'deltaSharingQueryTableChanges') AND response.status_code IN (401, 403) AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL '24' HOUR), failed_data_access_agg AS (SELECT window_start, window_end, email, metastore_id, LISTAGG(workspace_id, ',') AS workspace_ids, LISTAGG(action_name, ',') AS action_names, LISTAGG(securable, ',') AS securables, LISTAGG(error_message, ',') AS errors, COUNT(*) AS total FROM failed_data_access GROUP BY 1, 2, 3, 4) SELECT * FROM failed_data_access_agg WHERE total > 15 ORDER BY total DESC;\n"},
	{
		Name:        "destructive_activities_last_90_days",
		Description: "Monitor destructive activities over the last 90 days.",
		Query:       "SELECT event_date, user_identity.email, IF(json_extract(request_params, '$.workspace_id') IS NOT NULL, json_extract(request_params, '$.workspace_id'), workspace_id) AS workspace_id, service_name, action_name, COUNT(*) AS num_destructive_activities FROM read_parquet(S3_PRESIGNED_URL) WHERE event_date >= current_date - INTERVAL '90' DAY AND user_identity.email NOT IN ('System-User') AND (starts_with(action_name, 'delete') OR contains(lower(action_name), 'delete') OR contains(lower(action_name), 'trash')) GROUP BY 1, 2, 3, 4, 5 ORDER BY event_date DESC",
	},
	{
		Name:        "changes_to_workspace_configuration",
		Description: "Detect changes to workspace configuration in the last 24 hours.",
		Query:       "SELECT event_time, user_identity.email, workspace_id, json_extract(request_params, '$.workspaceConfKeys') AS workspaceConfKeys, json_extract(request_params, '$.workspaceConfValues') AS workspaceConfValues, count(*) AS total FROM read_parquet(S3_PRESIGNED_URL) WHERE action_name = 'workspaceConfEdit' AND event_time >= CAST(now() AS TIMESTAMP) - INTERVAL '24' HOUR GROUP BY 1, 2, 3, 4, 5 ORDER BY event_time DESC;",
	},
}

func listPredefinedQueries() {
	fmt.Println("Available monitoring queries:")
	for i, query := range predefinedQueries {
		fmt.Printf("[%d] %s - %s\n", i+1, query.Name, query.Description)
	}
}

func runQueries(index int) {
	if index == 0 {
		for _, query := range predefinedQueries {
			executeMonitoringQuery(query.Query)
		}
	} else {
		executeMonitoringQuery(predefinedQueries[index-1].Query)
	}
}

func executeMonitoringQuery(sqlQuery string) {
	preSignedURL, err := internal.FetchDeltaPreSignedURL("audit_logs", "audit", "logging", nil, 1000)
	if err != nil {
		log.Fatalf("Error fetching pre-signed URL: %v", err)
	}

	err = internal.ExecuteQueryOnParquet(sqlQuery, preSignedURL, "")
	if err != nil {
		log.Fatalf("Failed to execute monitoring query: %v", err)
	}
}

func resetMonitorFlags(cmd *cobra.Command) {
	cmd.Flags().Set("run", "0")
	cmd.Flags().Set("list", "false")
	cmd.Flags().Set("export", "")
}

func init() {
	monitorCmd.Flags().Int("run", 0, "Run a specific monitoring query by its number or '0' for all queries.")
	monitorCmd.Flags().Bool("list", false, "List all available monitoring queries.")
	monitorCmd.Flags().String("export", "", "Export the results to a CSV file (e.g., 'audit_results.csv')")
	rootCmd.AddCommand(monitorCmd)
}
