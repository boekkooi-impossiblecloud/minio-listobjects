package cmd

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/minio/cli"
	"github.com/minio/pkg/v2/ellipses"
	"github.com/valyala/bytebufferpool"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

var listObjectsFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "bucket",
		Usage: "the bucket to list objects for",
	},
	cli.BoolFlag{
		Name:  "versioned",
		Usage: "specify if the bucket is versioned",
	},
}

var listObjectsCmd = cli.Command{
	Name:   "list-objects",
	Usage:  "list all the objects of a bucket on the disks",
	Flags:  append(listObjectsFlags, GlobalFlags...),
	Action: listObjectsMain,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
     {{.Prompt}} {{.HelpName}} /mnt/data{1...64}
`,
}

func listObjectsMain(cliCtx *cli.Context) {
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
				disks = append(disks, ds...)
			}
		} else {
			disks = append(disks, arg)
		}
	}
	if len(disks) == 0 {
		logger.Error("No disks provided")
		return
	}

	if !cliCtx.IsSet("bucket") {
		logger.Error("bucket parameter is required")
		cli.ShowCommandHelpAndExit(cliCtx, cliCtx.Command.Name, 1)
		return
	}
	if !cliCtx.IsSet("versioned") {
		logger.Error("versioned parameter is required")
		cli.ShowCommandHelpAndExit(cliCtx, cliCtx.Command.Name, 1)
		return
	}

	bucket := cliCtx.String("bucket")
	versioned := cliCtx.Bool("versioned")
	runID := strconv.FormatInt(time.Now().Unix(), 10)

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

	for _, disk := range disks {
		createBucketObjectListForDisk(ctx, runID, disk, bucket, versioned)
		logger.Info("Done listing objects on disk " + disk)
	}
	logger.Info("Done listing objects for " + bucket)
}

func createBucketObjectListForDisk(ctx context.Context, runID, disk, bucket string, versioned bool) {
	name := bucket + "_" + runID + strings.Replace(disk, "/", "_", -1) + ".csv"
	fp, err := os.Create(name)
	if err != nil {
		logger.Fatal(err, "Unable to create file"+name)
	}
	defer func() {
		err = fp.Close()
		logger.FatalIf(err, "Failed to close list objects file for "+bucket+" on disk "+disk)
	}()

	csvWriter := csv.NewWriter(fp)
	defer csvWriter.Flush()

	r := readStorage{
		drivePath: disk,
		legacy:    globalServerCtxt.Layout.legacy,

		rotational: true,
		walkMu:     &sync.Mutex{},
		walkReadMu: &sync.Mutex{},
	}
	err = r.WalkDir(ctx, bucket, versioned, csvWriter)
	if err != nil {
		logger.Fatal(err, "Unable to list objects in "+bucket+" on disk "+disk)
	}
}

type readStorage struct {
	drivePath string
	legacy    bool

	// mutex to prevent concurrent read operations overloading walks.
	rotational bool
	walkMu     *sync.Mutex
	walkReadMu *sync.Mutex
}

// WalkDir is a striped down version of xlStorage.WalkDir
func (s *readStorage) WalkDir(ctx context.Context, bucket string, versioned bool, writer *csv.Writer) error {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(bucket)
	if err != nil {
		return err
	}

	send := func(entry metaCacheEntry) (err error) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Exclude directories
			if entry.isDir() {
				return nil
			}
			if versioned {
				var fiv FileInfoVersions
				fiv, err = entry.fileInfoVersions(bucket)
				if err != nil {
					return err
				}
				for _, version := range fiv.Versions {
					err = writer.Write([]string{entry.name, version.VersionID, strconv.FormatBool(version.Deleted)})
					if err != nil {
						return err
					}
				}
				return nil
			}
			// Exclude directories
			if entry.isObjectDir() && entry.isLatestDeletemarker() {
				return nil
			}
			// Exclude deleted objects
			if entry.isObject() && entry.isLatestDeletemarker() && !entry.isObjectDir() {
				return nil
			}
			return writer.Write([]string{entry.name})
		}
	}

	var scanDir func(path string) error
	scanDir = func(current string) (err error) {
		// Skip forward, if requested...
		sb := bytebufferpool.Get()
		defer func() {
			sb.Reset()
			bytebufferpool.Put(sb)
		}()

		if contextCanceled(ctx) {
			return ctx.Err()
		}

		if s.walkMu != nil {
			s.walkMu.Lock()
		}
		entries, err := s.ListDir(ctx, "", bucket, current, -1)
		if s.walkMu != nil {
			s.walkMu.Unlock()
		}
		if err != nil {
			// Folder could have gone away in-between
			if err != errVolumeNotFound && err != errFileNotFound {
				logger.LogOnceIf(ctx, err, "metacache-walk-scan-dir")
			}
			if err == errFileNotFound && current == "" {
				err = errFileNotFound
			} else {
				err = nil
			}
			return err
		}

		if len(entries) == 0 {
			return nil
		}
		dirObjects := make(map[string]struct{})

		// Avoid a bunch of cleanup when joining.
		current = strings.Trim(current, SlashSeparator)
		for i, entry := range entries {
			if hasSuffixByte(entry, SlashSeparatorChar) {
				if strings.HasSuffix(entry, globalDirSuffixWithSlash) {
					// Add without extension so it is sorted correctly.
					entry = strings.TrimSuffix(entry, globalDirSuffixWithSlash) + slashSeparator
					dirObjects[entry] = struct{}{}
					entries[i] = entry
					continue
				}
				// Trim slash, since we don't know if this is folder or object.
				entries[i] = entries[i][:len(entry)-1]
				continue
			}
			// Do not retain the file.
			entries[i] = ""

			if contextCanceled(ctx) {
				return ctx.Err()
			}
			// If root was an object return it as such.
			if HasSuffix(entry, xlStorageFormatFile) {
				var meta metaCacheEntry
				if s.walkReadMu != nil {
					s.walkReadMu.Lock()
				}
				meta.metadata, err = s.readMetadata(ctx, pathJoinBuf(sb, volumeDir, current, entry))
				if s.walkReadMu != nil {
					s.walkReadMu.Unlock()
				}
				diskHealthCheckOK(ctx, err)
				if err != nil {
					// It is totally possible that xl.meta was overwritten
					// while being concurrently listed at the same time in
					// such scenarios the 'xl.meta' might get truncated
					if !IsErrIgnored(err, io.EOF, io.ErrUnexpectedEOF) {
						logger.LogOnceIf(ctx, err, "metacache-walk-read-metadata")
					}
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFile)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoinBuf(sb, current, meta.name)
				meta.name = decodeDirObject(meta.name)

				return send(meta)
			}
			// Check legacy.
			if HasSuffix(entry, xlStorageFormatFileV1) && s.legacy {
				var meta metaCacheEntry
				meta.metadata, err = xioutil.ReadFile(pathJoinBuf(sb, volumeDir, current, entry))
				diskHealthCheckOK(ctx, err)
				if err != nil {
					if !IsErrIgnored(err, io.EOF, io.ErrUnexpectedEOF) {
						logger.LogIf(ctx, err)
					}
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFileV1)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoinBuf(sb, current, meta.name)

				return send(meta)
			}
			// Skip all other files.
		}

		// Process in sort order.
		sort.Strings(entries)
		dirStack := make([]string, 0, 5)

		for _, entry := range entries {
			if entry == "" {
				continue
			}
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			meta := metaCacheEntry{name: pathJoinBuf(sb, current, entry)}

			// If directory entry on stack before this, pop it now.
			for len(dirStack) > 0 && dirStack[len(dirStack)-1] < meta.name {
				pop := dirStack[len(dirStack)-1]
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					err = send(metaCacheEntry{name: pop})
					if err != nil {
						return err
					}
				}

				// Recursive Scan folder we found. Should be in correct sort order where we are.
				err = scanDir(pop)
				if err = scanDir(pop); err != nil {
					return err
				}

				dirStack = dirStack[:len(dirStack)-1]
			}

			// All objects will be returned as directories, there has been no object check yet.
			// Check it by attempting to read metadata.
			_, isDirObj := dirObjects[entry]
			if isDirObj {
				meta.name = meta.name[:len(meta.name)-1] + globalDirSuffixWithSlash
			}

			if s.walkReadMu != nil {
				s.walkReadMu.Lock()
			}
			meta.metadata, err = s.readMetadata(ctx, pathJoinBuf(sb, volumeDir, meta.name, xlStorageFormatFile))
			if s.walkReadMu != nil {
				s.walkReadMu.Unlock()
			}
			diskHealthCheckOK(ctx, err)
			switch {
			case err == nil:
				// It was an object
				if isDirObj {
					meta.name = strings.TrimSuffix(meta.name, globalDirSuffixWithSlash) + slashSeparator
				}
				if err := send(meta); err != nil {
					return err
				}
			case osIsNotExist(err), isSysErrIsDir(err):
				if s.legacy {
					meta.metadata, err = xioutil.ReadFile(pathJoinBuf(sb, volumeDir, meta.name, xlStorageFormatFileV1))
					diskHealthCheckOK(ctx, err)
					if err == nil {
						// It was an object
						if err := send(meta); err != nil {
							return err
						}
						continue
					}
				}

				// NOT an object, append to stack (with slash)
				// If dirObject, but no metadata (which is unexpected) we skip it.
				if !isDirObj && !isDirEmpty(pathJoinBuf(sb, volumeDir, meta.name)) {
					dirStack = append(dirStack, meta.name+slashSeparator)
				}
			case isSysErrNotDir(err):
				// skip
			}
		}

		// If directory entry left on stack, pop it now.
		for len(dirStack) > 0 {
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			pop := dirStack[len(dirStack)-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				err = send(metaCacheEntry{name: pop})
				if err != nil {
					return err
				}
			}

			// Recursive Scan folder we found. Should be in correct sort order where we are.
			if err = scanDir(pop); err != nil {
				return err
			}

			dirStack = dirStack[:len(dirStack)-1]
		}
		return nil
	}

	return scanDir("")
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing SlashSeparator.
func (s *readStorage) ListDir(ctx context.Context, origvolume, volume, dirPath string, count int) (entries []string, err error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
	}

	if origvolume != "" {
		if !skipAccessChecks(origvolume) {
			origvolumeDir, err := s.getVolDir(origvolume)
			if err != nil {
				return nil, err
			}
			if err = Access(origvolumeDir); err != nil {
				return nil, convertAccessError(err, errVolumeAccessDenied)
			}
		}
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	dirPathAbs := pathJoin(volumeDir, dirPath)
	if count > 0 {
		entries, err = readDirN(dirPathAbs, count)
	} else {
		entries, err = readDir(dirPathAbs)
	}
	if err != nil {
		if errors.Is(err, errFileNotFound) && !skipAccessChecks(volume) {
			if ierr := Access(volumeDir); ierr != nil {
				return nil, convertAccessError(ierr, errVolumeAccessDenied)
			}
		}
		return nil, err
	}

	return entries, nil
}

// getVolDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems. If volume is not found
// an error is generated.
func (s *readStorage) getVolDir(volume string) (string, error) {
	if volume == "" || volume == "." || volume == ".." {
		return "", errVolumeNotFound
	}
	volumeDir := pathJoin(s.drivePath, volume)
	return volumeDir, nil
}

func (s *readStorage) readMetadata(ctx context.Context, itemPath string) ([]byte, error) {
	return xioutil.WithDeadline[[]byte](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) ([]byte, error) {
		buf, _, err := s.readMetadataWithDMTime(ctx, itemPath)
		return buf, err
	})
}

// readsMetadata and returns disk mTime information for xl.meta
func (s *readStorage) readMetadataWithDMTime(ctx context.Context, itemPath string) ([]byte, time.Time, error) {
	if contextCanceled(ctx) {
		return nil, time.Time{}, ctx.Err()
	}

	if err := checkPathLength(itemPath); err != nil {
		return nil, time.Time{}, err
	}

	f, err := OpenFile(itemPath, readMode, 0o666)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}
	if stat.IsDir() {
		return nil, time.Time{}, &os.PathError{
			Op:   "open",
			Path: itemPath,
			Err:  syscall.EISDIR,
		}
	}
	buf, err := readXLMetaNoData(f, stat.Size())
	if err != nil {
		return nil, stat.ModTime().UTC(), fmt.Errorf("%w -> %s", err, itemPath)
	}
	return buf, stat.ModTime().UTC(), err
}
