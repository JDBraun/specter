package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"specter/internal"
	"strings"
)

var searchCmd = &cobra.Command{
	Use:   "search",
	Short: "Search the audit logs with filters",
	Long:  "Search the Delta Share audit logs based on user identity, service name, action name, event time, and more. Optionally export the results to a CSV file.",
	Run: func(cmd *cobra.Command, args []string) {
		predicateHints := buildPredicateHints(cmd)

		if len(predicateHints) == 0 {
			log.Fatal("At least one search flag must be provided.")
		}

		whereClause := strings.Join(predicateHints, " AND ")
		limitHint := 1000

		preSignedURL, err := internal.FetchDeltaPreSignedURL("audit_logs", "audit", "logging", predicateHints, limitHint)
		if err != nil {
			log.Fatalf("Error fetching pre-signed URL: %v", err)
		}

		sqlQuery := fmt.Sprintf("SELECT * FROM read_parquet(S3_PRESIGNED_URL) WHERE %s", whereClause)
		exportFile, _ := cmd.Flags().GetString("export")

		err = internal.ExecuteQueryOnParquet(sqlQuery, preSignedURL, exportFile) // Set isFullQuery to false
		if err != nil {
			log.Fatalf("Failed to execute query: %v", err)
		}

		resetSearchFlags(cmd)
	},
}

func buildPredicateHints(cmd *cobra.Command) []string {
	predicateHints := []string{}

	user, _ := cmd.Flags().GetString("user")
	service, _ := cmd.Flags().GetString("service")
	actionName, _ := cmd.Flags().GetString("action")
	date, _ := cmd.Flags().GetString("date")
	sourceIP, _ := cmd.Flags().GetString("source_ip")

	if user != "" {
		predicateHints = append(predicateHints, fmt.Sprintf("user_identity.email = '%s'", user))
	}
	if service != "" {
		predicateHints = append(predicateHints, fmt.Sprintf("service_name = '%s'", service))
	}
	if actionName != "" {
		predicateHints = append(predicateHints, fmt.Sprintf("action_name = '%s'", actionName))
	}
	if date != "" {
		predicateHints = append(predicateHints, fmt.Sprintf("event_time > '%s'", date))
	}
	if sourceIP != "" {
		predicateHints = append(predicateHints, fmt.Sprintf("source_ip_address = '%s'", sourceIP))
	}

	return predicateHints
}

func resetSearchFlags(cmd *cobra.Command) {
	cmd.Flags().Set("user", "")
	cmd.Flags().Set("service", "")
	cmd.Flags().Set("action", "")
	cmd.Flags().Set("date", "")
	cmd.Flags().Set("source_ip", "")
	cmd.Flags().Set("export", "")
}

func init() {
	searchCmd.Flags().String("user", "", "Filter by user email (e.g., user@email.com)")
	searchCmd.Flags().String("service", "", "Filter by service name (e.g., clusters)")
	searchCmd.Flags().String("action", "", "Filter by action name (e.g., delete)")
	searchCmd.Flags().String("date", "", "Filter by date (e.g., 2024-10-09)")
	searchCmd.Flags().String("source_ip", "", "Filter by source IP address")
	searchCmd.Flags().String("export", "", "Export the results to a CSV file (e.g., audit_results.csv)")

	rootCmd.AddCommand(searchCmd)
}
