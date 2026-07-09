package cmd

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/lemmego/tasker"
	"github.com/lemmego/tasker/supervisor"
)

type AppLike interface {
	AddService(any)
	Service(any) any
}

func WorkCommand(a AppLike) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tasker:work",
		Short: "Start the tasker worker",
		Run: func(cmd *cobra.Command, args []string) {
			mgr := tasker.Global()
			if mgr == nil {
				slog.Error("no tasker manager configured")
				os.Exit(1)
			}

			queues, _ := cmd.Flags().GetStringSlice("queue")
			workers, _ := cmd.Flags().GetInt("workers")

			cfg := supervisor.DefaultConfig()
			cfg.Queues = make(map[tasker.QueueName]supervisor.QueueConfig)

			for _, q := range queues {
				cfg.Queues[tasker.QueueName(q)] = supervisor.QueueConfig{
					MaxWorkers: workers,
					MinWorkers: 1,
				}
			}

			if len(cfg.Queues) == 0 {
				cfg.Queues["default"] = supervisor.QueueConfig{
					MaxWorkers: workers,
					MinWorkers: 1,
				}
			}

			sup := supervisor.New(mgr, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := sup.Start(ctx); err != nil {
				slog.Error("failed to start supervisor", "error", err)
				os.Exit(1)
			}

			slog.Info("tasker worker started")

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh

			slog.Info("shutting down tasker worker")
			sup.Stop(ctx)
		},
	}

	cmd.Flags().StringSliceP("queue", "q", []string{"default"}, "Queues to listen on")
	cmd.Flags().IntP("workers", "w", 3, "Number of workers per queue")

	return cmd
}
