package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use: "agent",
}

func init() {
	var configPath string
	RootCmd.Flags().StringVarP(&configPath, "config", "c", "", "path to config file")

	RootCmd.Run = func(cmd *cobra.Command, args []string) {
		serve(configPath)
	}
}
