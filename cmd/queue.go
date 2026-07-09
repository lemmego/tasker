package cmd

import (
	"github.com/spf13/cobra"

	"github.com/lemmego/tasker"
)

func QueueCommand(a interface{}) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tasker:queue",
		Short: "Manage tasker queues",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all queues and their stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr := tasker.Global()
			if mgr == nil {
				return nil
			}

			queues := []tasker.QueueName{"default"}
			for _, q := range queues {
				stats, err := mgr.QueueStats(cmd.Context(), q)
				if err != nil {
					continue
				}
				cmd.Printf("Queue: %s\n", stats.Queue)
				cmd.Printf("  Available: %d\n", stats.Available)
				cmd.Printf("  Running:   %d\n", stats.Running)
				cmd.Printf("  Completed: %d\n", stats.Completed)
				cmd.Printf("  Failed:    %d\n", stats.Failed)
				cmd.Printf("  Retryable: %d\n", stats.Retryable)
				cmd.Printf("  Scheduled: %d\n", stats.Scheduled)
				cmd.Println()
			}
			return nil
		},
	})

	return cmd
}
