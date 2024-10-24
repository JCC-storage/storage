package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:   "coordinator",
	Short: "Coordinator service for storage",
	Long:  `Coordinator service for storage`,
}

func init() {
	var configPath string
	RootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file")

	RootCmd.Run = func(cmd *cobra.Command, args []string) {
		serve(configPath)
	}
}
