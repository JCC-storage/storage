package cmdline

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
)

func init() {
	cmd := &cobra.Command{
		Use: "sysevent",
	}

	rootCmd.AddCommand(cmd)

	outputJSON := rootCmd.Flags().BoolP("json", "j", false, "output in JSON format")
	cmd.AddCommand(&cobra.Command{
		Use:   "watch",
		Short: "Watch system events",
		Run: func(cmd *cobra.Command, args []string) {
			watchSysEvent(*outputJSON)
		},
	})
}

func watchSysEvent(outputJSON bool) {
	host, err := sysevent.NewWatcherHost(sysevent.ConfigFromMQConfig(config.Cfg().RabbitMQ))
	if err != nil {
		fmt.Println(err)
		return
	}

	ch := host.Start()
	host.AddWatcherFn(func(event sysevent.SysEvent) {
		if outputJSON {
			data, err := serder.ObjectToJSON(event)
			if err != nil {
				fmt.Fprintf(os.Stderr, "serializing event: %v\n", err)
				return
			}

			fmt.Println(string(data))
		} else {
			fmt.Println(event)
		}
	})
	for {
		e, err := ch.Receive().Wait(context.Background())
		if err != nil {
			fmt.Println(err)
			return
		}

		switch e := e.(type) {
		case sysevent.PublishError:
			fmt.Fprintf(os.Stderr, "Publish error: %v\n", e.Err)

		case sysevent.PublisherExited:
			if e.Err != nil {
				fmt.Fprintf(os.Stderr, "Publisher exited with error: %v\n", e.Err)
			}
			return

		case sysevent.OtherError:
			fmt.Fprintf(os.Stderr, "Other error: %v\n", e.Err)
		}
	}
}
