package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "specter",
	Short: "Specter CLI",
	Long:  `Specter CLI for querying Databricks Delta Share audit logs`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Specter CLI is running")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
