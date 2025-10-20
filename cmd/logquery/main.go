package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	logclient "mp3-g02/logquerier/client"
	logcommon "mp3-g02/logquerier/common"
	"os"
	"strings"
	"time"
)

func main() {
	grepCmd := flag.String("grep", "", "Exact grep command to execute remotely (e.g. \"grep -E 'SUSPECT|FAILED'\")")
	hostsCSV := flag.String("hosts", "", "Comma-separated list of hosts or host:port entries")
	hostsFile := flag.String("hosts-file", logclient.DefaultHostsFile(), "Path to a hosts file (one host per line)")
	noHostsFile := flag.Bool("no-hosts-file", false, "Ignore the hosts file even if specified")
	port := flag.Int("port", logcommon.DefaultPort, "Log querier server port")
	fileType := flag.String("file", logcommon.DefaultFileType, "Logical log key to query (default 'node')")
	timeout := flag.Duration("timeout", 5*time.Second, "Per-host connect timeout")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "logquery streams grep output from all log querier servers.\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage:\n  logquery -grep \"grep -E 'PATTERN'\" [flags]\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if strings.TrimSpace(*grepCmd) == "" {
		flag.Usage()
		log.Fatal("missing required -grep command")
	}

	var hosts []string
	if trimmed := strings.TrimSpace(*hostsCSV); trimmed != "" {
		for _, part := range strings.Split(trimmed, ",") {
			if h := strings.TrimSpace(part); h != "" {
				hosts = append(hosts, h)
			}
		}
	}

	hf := *hostsFile
	if *noHostsFile {
		hf = ""
	}

	opts := logclient.Options{
		Hosts:     hosts,
		HostsFile: hf,
		Port:      *port,
		Request: logcommon.ServerRequest{
			Input:    *grepCmd,
			FileType: *fileType,
		},
		Output:    os.Stdout,
		ErrOutput: os.Stderr,
		Timeout:   *timeout,
	}

	if err := logclient.Run(context.Background(), opts); err != nil {
		log.Fatalf("logquery: %v", err)
	}
}
