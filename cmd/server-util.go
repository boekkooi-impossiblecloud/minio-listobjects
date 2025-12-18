// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/minio/cli"

	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/hash/sha256"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

var UtilFlags = append(ServerFlags,
	cli.StringFlag{
		Name:  "bucket",
		Usage: "The bucket to list",
	},
)

var utilCmd = cli.Command{
	Name:               "util",
	Usage:              "",
	Flags:              append(UtilFlags, GlobalFlags...),
	Action:             utilMain,
	CustomHelpTemplate: ``,
}

// serverMain handler called for 'minio server' command.
func utilMain(ctx *cli.Context) {
	var warnings []string

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go handleSignals()

	setDefaultProfilerRates()

	// Initialize globalConsoleSys system
	bootstrapTrace("newConsoleLogger", func() {
		globalConsoleSys = NewConsoleLogger(GlobalContext)
		logger.AddSystemTarget(GlobalContext, globalConsoleSys)

		// Set node name, only set for distributed setup.
		globalConsoleSys.SetNodeName(globalLocalNodeName)
	})

	// Always load ENV variables from files first.
	loadEnvVarsFromFiles()

	// Handle all server command args and build the disks layout
	bootstrapTrace("serverHandleCmdArgs", func() {
		err := buildServerCtxt(ctx, &globalServerCtxt)
		logger.FatalIf(err, "Unable to prepare the list of endpoints")

		serverHandleCmdArgs(globalServerCtxt)
	})

	// DNS cache subsystem to reduce outgoing DNS requests
	runDNSCache(ctx)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Load the root credentials from the shell environment or from
	// the config file if not defined, set the default one.
	loadRootCredentials()

	// Perform any self-tests
	bootstrapTrace("selftests", func() {
		bitrotSelfTest()
		erasureSelfTest()
		compressSelfTest()
	})

	// Initialize KMS configuration
	bootstrapTrace("handleKMSConfig", handleKMSConfig)

	// Initialize all help
	bootstrapTrace("initHelp", initHelp)

	// Initialize all sub-systems
	bootstrapTrace("initAllSubsystems", func() {
		initAllSubsystems(GlobalContext)
	})

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistErasure {
		if globalEndpoints.HTTPS() && !globalIsTLS {
			logger.Fatal(config.ErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.HTTPS() && globalIsTLS {
			logger.Fatal(config.ErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	// Set system resources to maximum.
	bootstrapTrace("setMaxResources", func() {
		_ = setMaxResources()
	})

	// Verify kernel release and version.
	if oldLinux() {
		warnings = append(warnings, color.YellowBold("- Detected Linux kernel version older than 4.0.0 release, there are some known potential performance problems with this kernel version. MinIO recommends a minimum of 4.x.x linux kernel version for best performance"))
	}

	maxProcs := runtime.GOMAXPROCS(0)
	cpuProcs := runtime.NumCPU()
	if maxProcs < cpuProcs {
		warnings = append(warnings, color.YellowBold("- Detected GOMAXPROCS(%d) < NumCPU(%d), please make sure to provide all PROCS to MinIO for optimal performance", maxProcs, cpuProcs))
	}

	// Initialize gridn
	bootstrapTrace("initGrid", func() {
		logger.FatalIf(initGlobalGrid(GlobalContext, globalEndpoints), "Unable to configure server grid RPC services")
	})

	// Allow grid to start after registering all services.
	xioutil.SafeClose(globalGridStart)

	if globalIsDistErasure {
		bootstrapTrace("verifying system configuration", func() {
			// Additionally in distributed setup, validate the setup and configuration.
			if err := verifyServerSystemConfig(GlobalContext, globalEndpoints, globalGrid.Load()); err != nil {
				logger.Fatal(err, "Unable to start the server")
			}
		})
	}

	var newObject ObjectLayer
	bootstrapTrace("newObjectLayer", func() {
		var err error
		newObject, err = newObjectLayer(GlobalContext, globalEndpoints)
		if err != nil {
			logFatalErrs(err, Endpoint{}, true)
		}
	})

	for _, n := range globalNodes {
		nodeName := n.Host
		if n.IsLocal {
			nodeName = globalLocalNodeName
		}
		nodeNameSum := sha256.Sum256([]byte(nodeName + globalDeploymentID()))
		globalNodeNamesHex[hex.EncodeToString(nodeNameSum[:])] = struct{}{}
	}

	bucketName := ctx.String("bucket")
	if bucketName == "" {
		logger.Fatal(errors.New("missing bucket argument"), "use --bucket")
		return
	}

	func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		var err error
		var buckets []BucketInfo
		// List buckets to initialize bucket metadata sub-sys.
		bootstrapTrace("listBuckets", func() {
			for {
				buckets, err = newObject.ListBuckets(GlobalContext, BucketOptions{})
				if err != nil {
					if configRetriableErrors(err) {
						logger.Info("Waiting for list buckets to succeed to initialize buckets.. possible cause (%v)", err)
						time.Sleep(time.Duration(r.Float64() * float64(time.Second)))
						continue
					}
					logger.LogIf(GlobalContext, fmt.Errorf("Unable to list buckets to initialize bucket metadata sub-system: %w", err))
				}

				break
			}
		})

		// Initialize bucket metadata sub-system.
		bootstrapTrace("globalBucketMetadataSys.Init", func() {
			globalBucketMetadataSys.Init(GlobalContext, buckets, newObject)
		})
	}()

	bucket, err := newObject.GetBucketInfo(GlobalContext, bucketName, BucketOptions{})
	if err != nil {
		logger.FatalIf(err, "Unable to get bucket info")
	}
	fmt.Printf("Bucket: %s, Versioning: %t, ObjectLocking: %t\n", bucket.Name, bucket.Versioning, bucket.ObjectLocking)

	inCh := make(chan metaCacheEntry, metacacheBlockSize)
	go func() {
		opts := listPathOptions{
			Bucket:      bucketName,
			Prefix:      "",
			Separator:   "",
			Limit:       math.MaxInt,
			Marker:      "",
			InclDeleted: true,
			AskDisks:    globalAPIConfig.getListQuorum(),
			Versioned:   bucket.Versioning,
		}
		opts.setBucketMeta(GlobalContext)

		pool := newObject.(*erasureServerPools)
		err := pool.listMerged(GlobalContext, opts, inCh)
		if err != nil {
			logger.Error("listMerged: listing %s finished with %s", opts.ID, err)
		}
	}()

	output := "/tmp/" + bucketName + ".objects.csv"
	fp, err := os.Create(output)
	if err != nil {
		logger.Fatal(err, "failed to create csv file")
	}
	defer fp.Close()
	target := csv.NewWriter(fp)
	defer target.Flush()

	fmt.Printf("Writing records to %s\n", output)
	var record []string
	for entry := range inCh {
		// Skip directories
		if entry.isDir() || (!bucket.Versioning && entry.isObjectDir() && entry.isLatestDeletemarker()) {
			continue
		}

		fmt.Printf(".")
		if bucket.Versioning {
			fiv, err := entry.fileInfoVersions(bucketName)
			if err != nil {
				logger.Fatal(err, "fileInfoVersions: failed to get version of %s: %s", entry.name)
			}
			for _, version := range fiv.Versions {
				record = append(record, version.Name, version.VersionID)
				if err := target.Write(record); err != nil {
					logger.Fatal(err, "failed to write row for %s", entry.name)
				}
				record = record[:0]
			}
		} else {
			record = append(record, entry.name)
			if err := target.Write(record); err != nil {
				logger.Fatal(err, "failed to write row for %s", entry.name)
			}
			record = record[:0]
		}
	}
	fmt.Printf("\nDone\n")
}
