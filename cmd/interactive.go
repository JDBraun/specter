package cmd

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var interactiveCmd = &cobra.Command{
	Use:   "interactive",
	Short: "Enter interactive mode (REPL)",
	Long:  `Start an interactive session where you can continuously enter commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("welcome to Specter. Type 'exit' to leave.")
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("specter ")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)

			if input == "exit" {
				fmt.Println("Exiting interactive mode.")
				break
			}

			if input != "" {
				inputArgs := strings.Fields(input)
				rootCmd.SetArgs(inputArgs)
				if err := rootCmd.Execute(); err != nil {
					fmt.Printf("Error: %v\n", err)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(interactiveCmd)
}
