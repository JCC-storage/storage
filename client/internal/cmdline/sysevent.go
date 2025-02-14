package cmdline

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/storage/client/internal/config"
	"gitlink.org.cn/cloudream/storage/common/pkgs/sysevent"
)

func init() {
	cmd := &cobra.Command{
		Use: "sysevent",
	}

	rootCmd.AddCommand(cmd)

	cmd.AddCommand(&cobra.Command{
		Use:   "watch",
		Short: "Watch system events",
		Run: func(cmd *cobra.Command, args []string) {
			watchSysEvent(GetCmdCtx(cmd))
		},
	})
}

func watchSysEvent(cmdCtx *CommandContext) {
	host, err := sysevent.NewWatcherHost(sysevent.ConfigFromMQConfig(config.Cfg().RabbitMQ))
	if err != nil {
		fmt.Println(err)
		return
	}

	ch := host.Start()
	host.AddWatcherFn(func(event sysevent.SysEvent) {
		fmt.Println(event.String())
	})
	for {
		e, err := ch.Receive().Wait(context.Background())
		if err != nil {
			fmt.Println(err)
			return
		}

		switch e := e.(type) {
		case sysevent.PublishError:
			fmt.Printf("Publish error: %v\n", e.Err)

		case sysevent.PublisherExited:
			if e.Err != nil {
				fmt.Printf("Publisher exited with error: %v\n", e.Err)
			}
			return

		case sysevent.OtherError:
			fmt.Printf("Other error: %v\n", e.Err)
		}
	}
}
