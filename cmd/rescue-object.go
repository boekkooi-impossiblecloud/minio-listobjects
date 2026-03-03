package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minio/cli"
	"github.com/minio/pkg/v2/ellipses"

	"github.com/minio/minio/internal/logger"
)

var rescueObjectFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "object",
		Usage: "the bucket/path of the object to reconstruct",
	},
	cli.BoolFlag{
		Name:  "versioned",
		Usage: "specify if the bucket is versioned",
	},
}

var rescueObjectCmd = cli.Command{
	Name:   "rescue-object",
	Usage:  "reconstruct an object from it's shards",
	Flags:  append(rescueObjectFlags, GlobalFlags...),
	Action: rescueObjectMain,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
     {{.Prompt}} {{.HelpName}} --object "bucket/key" /mnt/data{1...64}
`,
}

func rescueObjectMain(cliCtx *cli.Context) {
	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	globalConsoleSys = NewConsoleLogger(GlobalContext)
	_ = logger.AddSystemTarget(GlobalContext, globalConsoleSys)

	if !cliCtx.Args().Present() || cliCtx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(cliCtx, cliCtx.Command.Name, 1)
	}

	var disks []string
	for _, arg := range cliCtx.Args() {
		if ellipses.HasEllipses(arg) {
			patterns, perr := ellipses.FindEllipsesPatterns(arg)
			if perr != nil {
				logger.Fatal(perr, "failed to find ellipsis patterns for "+arg)
			}
			for _, ds := range patterns.Expand() {
				p := new(strings.Builder)
				for _, d := range ds {
					p.WriteString(d)
				}
				disks = append(disks, p.String())
			}
		} else {
			disks = append(disks, arg)
		}
	}
	if len(disks) == 0 {
		logger.Error("No disks provided")
		return
	}

	if !cliCtx.IsSet("object") {
		logger.Error("object parameter is required")
		cli.ShowCommandHelpAndExit(cliCtx, cliCtx.Command.Name, 1)
		return
	}
	if !cliCtx.IsSet("versioned") {
		logger.Error("versioned parameter is required")
		cli.ShowCommandHelpAndExit(cliCtx, cliCtx.Command.Name, 1)
		return
	}

	object := cliCtx.String("object")
	versioned := cliCtx.Bool("versioned")
	//runID := strconv.FormatInt(time.Now().Unix(), 10)

	logger.Info("[" + time.Now().Format(time.RFC3339) + "] Rescue " + object + " versioned " + strconv.FormatBool(versioned))

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	go func() {
		select {
		case <-globalOSSignalCh:
			ctxCancel()
		case <-ctx.Done():
			return
		}
	}()

	//_, errs := readAllFileInfo(healCtx, storageDisks, "", bucket, object, versionID, false, false)

	//wg := sync.WaitGroup{}
	var erasureInfo *ErasureInfo
	var partSizes []int64
	var partReaders [][]io.ReaderAt
	for _, disk := range disks {
		diskObjectPath := path.Join(disk, object)
		metaDataPath := path.Join(diskObjectPath, xlStorageFormatFile)

		metaDataBytes, err := readMetadataFileWithDMTime(ctx, metaDataPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			logger.Error("failed to read metadata file %q: %s", metaDataPath, err.Error())
			continue
		}
		logger.Info("Found file %q", metaDataPath)

		metaData := new(xlMetaV2)
		err = metaData.Load(metaDataBytes)
		if err != nil {
			logger.Error("failed to parse metadata file %q: %s", metaDataPath, err.Error())
			continue
		}

		objFileInfo, err := metaData.ToFileInfo(disk, object, "", false, true)
		if err != nil {
			logger.Error("failed to convert metadata to file info for %q: %s", metaDataPath, err.Error())
			continue
		}

		if erasureInfo == nil {
			erasureInfo = &objFileInfo.Erasure
		} else if !objFileInfo.Erasure.Equal(*erasureInfo) {
			logger.Error("erasure info mismatch for %q: existing %v, new %v", metaDataPath, erasureInfo, objFileInfo.Erasure)
			continue
		}

		for _, part := range objFileInfo.Parts {
			for len(partSizes) < part.Number {
				partSizes = append(partSizes, 0)
				partReaders = append(partReaders, make([]io.ReaderAt, 8))
			}
			partI := part.Number - 1
			partSizes[partI] = part.Size

			partPath := pathJoin(diskObjectPath, objFileInfo.DataDir, fmt.Sprintf("part.%d", part.Number))
			partBytes, err := os.ReadFile(partPath)
			if err != nil {
				logger.Error("failed to read part file %q: %w", partPath, err)
				continue
			}

			partReaders[partI] = append(partReaders[part.Number-1], bytes.NewReader(partBytes))
			//checksumInfo := objFileInfo.Erasure.GetChecksumInfo(part.Number)
			//readers[index] = newBitrotReader(disk, objFileInfo.Data, bucket, partPath, tillOffset,
			//	checksumInfo.Algorithm, checksumInfo.Hash, erasure.ShardSize())

		}
	}

	if erasureInfo == nil {
		logger.Error("failed to find any metadata for object %q", object)
		return
	}

	erasure, err := NewErasure(ctx, erasureInfo.DataBlocks, erasureInfo.ParityBlocks, erasureInfo.BlockSize)
	if err != nil {
		logger.Error("failed to create erasure object for %q: %s", object, err.Error())
		return
	}

	wd, _ := os.Getwd()
	outputPath := path.Join(wd, path.Base(object))
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0x644)
	if err != nil {
		logger.Error("failed to create output file for %q: %s", outputPath, err.Error())
		return
	}
	defer output.Close()

	for i, partSize := range partSizes {
		written, err := erasure.Decode(ctx, output, partReaders[i], 0, partSize, partSize, nil)
		if err != nil {
			logger.Error("failed to decode part %d for %q: %s", i+1, object, err.Error())
			return
		}
		logger.Info("[" + time.Now().Format(time.RFC3339) + "] Written part " + strconv.Itoa(i+1) + " size " + strconv.FormatInt(written, 10))
	}
	logger.Info("[" + time.Now().Format(time.RFC3339) + "] Done")
}

// readMetadataFileWithDMTime and returns disk information of a xl.meta
func readMetadataFileWithDMTime(ctx context.Context, itemPath string) ([]byte, error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
	}

	if err := checkPathLength(itemPath); err != nil {
		return nil, err
	}

	f, err := OpenFile(itemPath, readMode, 0o666)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.IsDir() {
		return nil, &os.PathError{
			Op:   "open",
			Path: itemPath,
			Err:  syscall.EISDIR,
		}
	}
	buf, err := readXLMetaNoData(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("%w -> %s", err, itemPath)
	}
	return buf, err
}
