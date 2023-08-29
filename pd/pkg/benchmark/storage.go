package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	sbpClient "github.com/AutoMQ/pd/pkg/sbp/client"
)

const (
	_storageHeartbeatInterval = 5 * time.Second
	_storageStreamReplica     = 3
	_storageObjectSize        = 1 << 27 // 128MB
)

var storageCmd = &cobra.Command{
	Use:   "storage",
	Short: "Benchmark storage",
	Long:  "Benchmark storage tests the storage usage of PD.",
	RunE:  storageFunc,
}

var (
	storageClients                 int
	storageRangeServers            int
	storageStreams                 int
	storageRangePerStream          int
	storageObjectPerRange          int
	storageObjectSparseIndexLength int
	storageSkipMaintain            bool

	storageObjectSparseIndex []byte
	storageStreamCounter     atomic.Int32
	storageRangeCounter      atomic.Int32
	storageObjectCounter     atomic.Int32
)

func init() {
	RootCmd.AddCommand(storageCmd)

	storageCmd.Flags().IntVarP(&storageClients, "clients", "c", 10, "Total PD clients count")
	storageCmd.Flags().IntVarP(&storageRangeServers, "range-servers", "R", 10, "Total range servers count")
	storageCmd.Flags().IntVarP(&storageStreams, "streams", "s", 100, "Total streams count")
	storageCmd.Flags().IntVarP(&storageRangePerStream, "range-per-stream", "r", 10, "Total ranges per stream")
	storageCmd.Flags().IntVarP(&storageObjectPerRange, "object-per-range", "o", 64, "Total objects per range")
	storageCmd.Flags().IntVarP(&storageObjectSparseIndexLength, "object-index-length", "i", 64, "Sparse index length of each object")
	storageCmd.Flags().BoolVarP(&storageSkipMaintain, "skip-maintain", "M", false, "Skip compact and defragment etcd after benchmark")
}

func storageFunc(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageObjectSparseIndex = make([]byte, storageObjectSparseIndexLength)
	_, err := rand.Read(storageObjectSparseIndex)
	if err != nil {
		return errors.WithMessage(err, "generate sparse index")
	}

	closeHeartbeats := heartbeatLoop(ctx, storageRangeServers, _storageHeartbeatInterval)
	defer closeHeartbeats()
	clients, closeClients := newClients(ctx, storageClients, gLogger)
	defer closeClients()

	return benchStorage(ctx, clients)
}

func benchStorage(ctx context.Context, clients []sbpClient.Client) error {
	etcdClient, err := newEtcdClient()
	if err != nil {
		return err
	}
	defer func() {
		_ = etcdClient.Close()
	}()

	infoBefore, err := getEtcdInfo(ctx, etcdClient)

	gLogger.Info("storage benchmark started")
	bar = pb.Full.New(storageStreams + 2*storageStreams*storageRangePerStream + storageStreams*storageRangePerStream*storageObjectPerRange)
	bar.Start()

	streamCh := make(chan struct{})
	errCh := make(chan error, storageClients)

	wg := sync.WaitGroup{}
	for _, client := range clients {
		wg.Add(1)
		go func(client sbpClient.Client) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-streamCh:
					if !ok {
						return
					}
				}
				err := writeMetadata(ctx, client, bar)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(client)
	}

	for i := 0; i < storageStreams; i++ {
		select {
		case <-ctx.Done():
			return nil
		case err = <-errCh:
			break
		case streamCh <- struct{}{}:
		}
	}
	close(streamCh)

	wg.Wait()
	bar.Finish()
	gLogger.Info("storage benchmark finished", zap.Error(err))

	infoAfter, err := getEtcdInfo(ctx, etcdClient)
	if err != nil {
		return err
	}
	var infoAfterMaintain etcdInfo
	if !storageSkipMaintain {
		fmt.Println("Start to compact and defragment etcd...")
		err = etcdReleaseSpace(ctx, etcdClient)
		if err != nil {
			return err
		}
		infoAfterMaintain, err = getEtcdInfo(ctx, etcdClient)
		if err != nil {
			return err
		}
	}
	fmt.Println("Storage benchmark finished")
	fmt.Println("Created streams:", storageStreamCounter.Load())
	fmt.Println("Created ranges :", storageRangeCounter.Load())
	fmt.Println("Created objects:", storageObjectCounter.Load())
	fmt.Println("Before         :", infoBefore)
	fmt.Println("After          :", infoAfter)
	if !storageSkipMaintain {
		fmt.Println("After maintain :", infoAfterMaintain)
	}

	return err
}

func writeMetadata(ctx context.Context, client sbpClient.Client, bar *pb.ProgressBar) error {
	sID, err := createStream(ctx, client, _storageStreamReplica)
	storageStreamCounter.Add(1)
	bar.Increment()
	if err != nil {
		return errors.WithMessage(err, "create stream")
	}

	var offset int64
	var rIndex int32
	for rIndex < int32(storageRangePerStream) {
		err = createRange(ctx, client, sID, rIndex, offset)
		storageRangeCounter.Add(1)
		bar.Increment()
		if err != nil {
			return errors.WithMessage(err, "create range")
		}

		for o := 0; o < storageObjectPerRange; o++ {
			err = commitObject(ctx, client, sID, rIndex, offset, offset+1, _storageObjectSize, nil)
			storageObjectCounter.Add(1)
			bar.Increment()
			if err != nil {
				return errors.WithMessage(err, "commit object")
			}
			offset++
		}

		err = sealRange(ctx, client, sID, rIndex, offset)
		bar.Increment()
		if err != nil {
			return errors.WithMessage(err, "seal range")
		}

		rIndex++
	}

	return nil
}
