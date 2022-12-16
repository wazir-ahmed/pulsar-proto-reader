package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wazir-ahmed/pulsar-proto-reader/cmd/reader"
)

var option reader.ConnectorOption

var rootCmd = &cobra.Command{
	Use:   "pulsar-proto-reader",
	Short: "pulsar-proto-reader - Tool to read pulsar feeds encoded in protobuf format",
	Run: func(cmd *cobra.Command, args []string) {
		reader.PrintProtoFeeds(option)
	},
}

func init() {
	option = reader.ConnectorOption{}
	rootCmd.Flags().StringVarP(&option.URL, "url", "u", "pulsar://localhost:6650", "Pulsar URL")
	rootCmd.Flags().StringVar(&option.Topic, "topic", "t", "Pulsar topic (example: persistent://public/default/test)")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error - '%s'", err)
		os.Exit(1)
	}
}
