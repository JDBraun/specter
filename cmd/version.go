package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of Specter",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Specter CLI v1.0.0")
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
