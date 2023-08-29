package benchmark

import (
	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	RootCmd = &cobra.Command{
		Use:               "benchmark",
		Short:             "A low-level benchmark tool for PD",
		Long:              "benchmark is a low-level benchmark tool for PD. It uses SBP client directly",
		PersistentPreRunE: rootFunc,
	}
)

func rootFunc(cmd *cobra.Command, _ []string) error {
	if cmd.Flag("debug").Changed {
		lg, err := zap.NewDevelopment()
		if err != nil {
			return err
		}
		gLogger = lg
	}
	return nil
}

var (
	pdAddr  string
	etcdURL string

	bar *pb.ProgressBar

	gLogger = zap.NewNop()
)

func init() {
	RootCmd.PersistentFlags().StringVar(&pdAddr, "pd-addr", "127.0.0.1:12378", "PD address")
	RootCmd.PersistentFlags().StringVar(&etcdURL, "etcd-url", "http://127.0.0.1:12379", "etcd url")

	RootCmd.PersistentFlags().Bool("debug", false, "Enable debug mode")
}
