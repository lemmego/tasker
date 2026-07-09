package cmd

import (
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/lemmego/tasker"
)

func JobCommand(a interface{}) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tasker:job",
		Short: "Manage tasker jobs",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a failed job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr := tasker.Global()
			if mgr == nil {
				return nil
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return err
			}

			_, err = mgr.Retry(cmd.Context(), tasker.JobID(id))
			return err
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "cancel <id>",
		Short: "Cancel a pending/scheduled job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr := tasker.Global()
			if mgr == nil {
				return nil
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return err
			}

			_, err = mgr.Cancel(cmd.Context(), tasker.JobID(id))
			return err
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "clear",
		Short: "Clear all failed jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr := tasker.Global()
			if mgr == nil {
				return nil
			}

			states, _ := cmd.Flags().GetStringSlice("states")
			taskerStates := make([]tasker.State, len(states))
			for i, s := range states {
				taskerStates[i] = tasker.State(s)
			}

			_, err := mgr.Driver().Prune(cmd.Context(), time.Now(), taskerStates)
			return err
		},
	})

	cmd.Flags().StringSlice("states", []string{"failed", "cancelled"}, "Job states to clear")

	return cmd
}
