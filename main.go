package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"mp3-g02/detectors"
	"mp3-g02/hydfs" // MP3: Import HyDFS package
	logclient "mp3-g02/logquerier/client"
	logcommon "mp3-g02/logquerier/common"
	logserver "mp3-g02/logquerier/server"
	"mp3-g02/utils"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Controller is simplified to only manage PingAck
type Controller struct {
	pingAckManager *detectors.PingAckManager
	membership     *utils.MembershipList
	network        *utils.NetworkLayer
	suspicionMgr   *utils.SuspicionManager
}

func NewController(config utils.Config) (*Controller, error) {
	network := utils.NewNetworkLayer()
	membership := utils.NewMembershipList(config.NodeID)
	opts := utils.Options{
		SuspicionTimeout:   2 * time.Second,
		CheckInterval:      200 * time.Millisecond,
		RequireReports:     1,
		ConfirmedRetention: 3 * time.Second,
		OnSuspect: func(target utils.NodeID, inc int32, reporters []utils.NodeID) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = utils.Suspected
				m.Incarnation = inc
				m.SuspicionStart = time.Now()
			}
			recipients := make([]utils.NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recipients = append(recipients, mm.ID)
			}
			membership.Unlock()

			msg := utils.Message{
				Type:        utils.Suspect,
				Sender:      membership.LocalNode,
				Target:      target,
				Incarnation: inc,
			}
			for _, r := range recipients {
				network.Send(msg, r.Address())
			}
			log.Printf("SUSPECT: %s inc=%d reporters=%v", target, inc, reporters)
		},
		OnConfirm: func(target utils.NodeID, inc int32) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = utils.Failed
				m.Incarnation = inc
			}
			recipients := make([]utils.NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recipients = append(recipients, mm.ID)
			}
			membership.Unlock()

			msg := utils.Message{
				Type:        utils.Confirm,
				Sender:      membership.LocalNode,
				Target:      target,
				Incarnation: inc,
			}
			for _, r := range recipients {
				network.Send(msg, r.Address())
			}
			log.Printf("FAILED: %s inc=%d", target, inc)
		},
		OnClear: func(target utils.NodeID, inc int32) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = utils.Alive
				if inc >= m.Incarnation {
					m.Incarnation = inc
				}
				m.LastHeartbeat = time.Now()
				m.SuspicionStart = time.Time{}
			}
			recipients := make([]utils.NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recipients = append(recipients, mm.ID)
			}
			membership.Unlock()

			msg := utils.Message{
				Type:        utils.AliveMsg,
				Sender:      membership.LocalNode,
				Incarnation: inc,
			}
			for _, r := range recipients {
				network.Send(msg, r.Address())
			}
			log.Printf("CLEARED: %s inc=%d", target, inc)
		},
	}

	suspicionMgr := utils.NewSuspicionManager(membership, network, opts)
	// Only initialize the PingAckManager
	pingAckManager := detectors.NewPingAckManager(membership, network, suspicionMgr)

	// --- Hard-code suspicion to TRUE for MP3 ---
	// HyDFS relies on suspicion to avoid unnecessary re-replication.
	pingAckManager.SetSuspicion(true)
	log.Println("Suspicion-based failure detection is ENABLED.")
	// ---

	controller := &Controller{
		pingAckManager: pingAckManager,
		membership:     membership,
		network:        network,
		suspicionMgr:   suspicionMgr,
	}

	return controller, nil
}

func (c *Controller) Start() error {
	// Start network layer
	if err := c.network.Start(c.membership.LocalNode.Port); err != nil {
		return err
	}

	// Start suspicion manager with a background context
	c.suspicionMgr.Start(context.Background())

	// Start only the PingAckManager
	c.pingAckManager.Start()

	log.Println("Controller (PingAck) started")
	return nil
}

// JoinGroup joins the distributed group via introducer
func (c *Controller) JoinGroup(introducerAddr string) error {
	return c.pingAckManager.JoinGroup(introducerAddr)
}

func (c *Controller) Stop() {
	// Stop only the PingAckManager
	c.pingAckManager.Stop()

	c.suspicionMgr.Stop()
	c.network.Stop()
	log.Println("Controller stopped")
}

// StartCLI starts a command line interface for the controller
func StartCLI(controller *Controller) {
	// Enhanced CLI to provide basic functionality
	log.Println("CLI started - Controller is running")
	log.Printf("Node ID: %s", controller.membership.LocalNode)
	log.Printf("Listening on port: %d", controller.membership.LocalNode.Port)

	// Periodically log membership status
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		members := controller.membership.GetAllMembers()
		aliveCount := 0
		suspectedCount := 0
		failedCount := 0

		log.Printf("=== MEMBERSHIP STATUS ===")
		log.Printf("Total members: %d", len(members))

		for _, member := range members {
			timeSinceHeartbeat := time.Since(member.LastHeartbeat)
			statusInfo := ""

			switch member.Status {
			case utils.Alive:
				aliveCount++
				if timeSinceHeartbeat > 5*time.Second {
					statusInfo = fmt.Sprintf(" (stale: %v)", timeSinceHeartbeat)
				}
			case utils.Suspected:
				suspectedCount++
				timeSinceSuspicion := time.Since(member.SuspicionStart)
				statusInfo = fmt.Sprintf(" (suspected for: %v)", timeSinceSuspicion)
			case utils.Failed:
				failedCount++
				statusInfo = " (failed)"
			}

			log.Printf("  %s | %s | Inc:%d | LastHB:%v ago%s",
				member.ID, member.Status, member.Incarnation,
				timeSinceHeartbeat.Truncate(time.Millisecond), statusInfo)
		}

		log.Printf("Summary: %d alive, %d suspected, %d failed", aliveCount, suspectedCount, failedCount)

		// Log recent updates being propagated
		recentUpdates := controller.membership.GetRecentUpdates(10)
		if len(recentUpdates) > 0 {
			log.Printf("Recent updates (piggybacking): %d", len(recentUpdates))
			for _, update := range recentUpdates {
				log.Printf("%s -> %s (Inc:%d)", update.NodeID, update.Status, update.Incarnation)
			}
		}
		log.Printf("========================")
	}
}

// LeaveGroup triggers a voluntary leave
func (c *Controller) LeaveGroup() {
	c.pingAckManager.LeaveGroup()
}

func main() {
	var (
		port          = flag.Int("port", 8080, "UDP port to listen on")
		introducerIP  = flag.String("introducer", "", "Introducer IP:Port")
		isIntroducer  = flag.Bool("is-introducer", false, "Act as introducer")
		cmd           = flag.String("cmd", "", "Client command: list_mem, list_mem_ids, list_self, join, leave, display_suspects, grep_logs, create, get, append, merge, ls, liststore, getfromreplica, multiappend")
		controlPortIn = flag.Int("control-port", 0, "Control server port on localhost (default: port+10000)")
		// **** FIX: Removed arg1 and arg2 flags ****
		foreground = flag.Bool("foreground", false, "Run in foreground (do not daemonize)")
	)
	flag.Parse()

	controlPort := *controlPortIn
	if controlPort == 0 {
		controlPort = *port + 10000
	}

	// If -cmd is provided, act as a client and exit
	if *cmd != "" {
		// **** FIX: Pass flag.Args() which contains the positional arguments ****
		runClient(*cmd, controlPort, flag.Args())
		return
	}

	// Daemonize (background) unless foreground requested or already daemonized
	if !*foreground && os.Getenv("MP2_DAEMONIZED") != "1" {
		exe, err := os.Executable()
		if err != nil {
			log.Fatalf("cannot get executable: %v", err)
		}
		args := os.Args[1:]
		child := exec.Command(exe, args...)
		child.Env = append(os.Environ(), "MP2_DAEMONIZED=1")
		child.Stdin = nil
		if err := child.Start(); err != nil {
			log.Fatalf("failed to start daemon: %v", err)
		}

		fmt.Printf("Started mp2-node daemon pid=%d port=%d (control-port=%d)\n", child.Process.Pid, *port, controlPort)
		return
	}

	// Get local IP
	localIP := utils.GetLocalIP()
	nodeID := utils.NodeID{
		IP:        localIP,
		Port:      *port,
		Timestamp: time.Now().Unix(),
	}

	// Create controller (MP2)
	config := utils.Config{
		NodeID:         nodeID,
		IntroducerAddr: *introducerIP,
		IsIntroducer:   *isIntroducer,
	}

	controller, err := NewController(config)
	if err != nil {
		log.Fatalf("Failed to create controller: %v", err)
	}

	// Start the MP2 system
	if err := controller.Start(); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}

	workDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to determine working directory: %v", err)
	}

	// --- MP1 Log Querier Server Setup ---
	logFilePath := filepath.Join(workDir, "node.log")
	if f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND, 0o644); err != nil {
		log.Fatalf("Failed to ensure node.log exists: %v", err)
	} else {
		_ = f.Close()
	}

	logSrvCfg := logserver.Config{
		Addr:             fmt.Sprintf(":%d", logcommon.DefaultPort),
		LogPaths:         map[string]string{logcommon.DefaultFileType: "node.log"},
		DefaultFileType:  logcommon.DefaultFileType,
		WorkingDirectory: workDir,
	}
	logSrv, err := logserver.New(logSrvCfg)
	if err != nil {
		log.Fatalf("Failed to initialize log querier server: %v", err)
	}
	logSrvCtx, logSrvCancel := context.WithCancel(context.Background())
	defer func() {
		logSrvCancel()
		_ = logSrv.Close()
	}()
	go func() {
		if err := logSrv.Serve(logSrvCtx); err != nil {
			log.Printf("log querier server stopped: %v", err)
		}
	}()
	// --- End MP1 Setup ---

	// --- MP3 HyDFS Server Setup ---
	storageDir := filepath.Join(workDir, "hydfs_storage")
	if !*isIntroducer && *introducerIP != "" {
		log.Printf("Node is rejoining, clearing old HyDFS storage at %s", storageDir)
		os.RemoveAll(storageDir)
	}
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create HyDFS storage directory: %v", err)
	}

	hyConfig := hydfs.Config{
		StoragePath: storageDir,
		ControlPort: controlPort,
	}
	hyServer, err := hydfs.NewServer(hyConfig, controller.membership, nodeID, controlPort)
	if err != nil {
		log.Fatalf("Failed to create HyDFS server: %v", err)
	}
	hyServer.Start() // Start background tasks
	// --- End MP3 Setup ---

	// Start control server (daemon API)
	ctl := NewControlServer(controller, hyServer, controlPort)
	ctl.Start()

	// Join the group if not introducer
	if !*isIntroducer && *introducerIP != "" {
		if err := controller.JoinGroup(*introducerIP); err != nil {
			log.Printf("Failed to join group: %v", err)
		} else {
			log.Printf("Joined the group")
		}
	}

	go StartCLI(controller)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	// stop control server first
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctl.Stop(shutdownCtx)
	hyServer.Stop()   // Stop MP3 service
	controller.Stop() // Stop MP2 service
}

// --- Control Server (HTTP API) ---

type ControlServer struct {
	controller *Controller   // MP2 Controller
	hydfs      *hydfs.Server // MP3 HyDFS Server
	srv        *http.Server
}

func NewControlServer(c *Controller, hyServer *hydfs.Server, controlPort int) *ControlServer {
	mux := http.NewServeMux()
	cs := &ControlServer{
		controller: c,
		hydfs:      hyServer,
	}

	// --- MP2 Endpoints ---
	mux.HandleFunc("/list_mem", cs.handleListMem)
	mux.HandleFunc("/list_mem_ids", cs.handleListMemIDs)
	mux.HandleFunc("/list_self", cs.handleListSelf)
	mux.HandleFunc("/display_suspects", cs.handleDisplaySuspects)
	mux.HandleFunc("/join", cs.handleJoin)
	mux.HandleFunc("/leave", cs.handleLeave)

	// --- MP3 Public Endpoints ---
	mux.HandleFunc("/create", cs.hydfs.HandleCreate)
	mux.HandleFunc("/get", cs.hydfs.HandleGet)
	mux.HandleFunc("/append", cs.hydfs.HandleAppend)
	mux.HandleFunc("/merge", cs.hydfs.HandleMerge)
	mux.HandleFunc("/ls", cs.hydfs.HandleLs)
	mux.HandleFunc("/liststore", cs.hydfs.HandleListStore)

	// --- MP3 Internal Endpoints ---
	mux.HandleFunc("/internal/write", cs.hydfs.HandleInternalWrite)
	mux.HandleFunc("/internal/get-meta", cs.hydfs.HandleInternalGetMeta)
	mux.HandleFunc("/internal/get-block", cs.hydfs.HandleInternalGetBlock)
	mux.HandleFunc("/internal/write-meta", cs.hydfs.HandleInternalWriteMeta)

	cs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", controlPort),
		Handler: mux,
	}
	return cs
}

func (cs *ControlServer) Start() {
	go func() {
		log.Printf("Control server listening on http://%s", cs.srv.Addr)
		if err := cs.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("control server error: %v", err)
		}
	}()
}

func (cs *ControlServer) Stop(ctx context.Context) {
	_ = cs.srv.Shutdown(ctx)
}

// --- MP2 API Handlers ---

func (cs *ControlServer) handleListMem(w http.ResponseWriter, r *http.Request) {
	members := cs.controller.membership.GetAllMembers()
	for _, member := range members {
		fmt.Fprintf(w, "  %s | %s | Inc:%d\n", member.ID, member.Status, member.Incarnation)
	}
}

func (cs *ControlServer) handleListMemIDs(w http.ResponseWriter, r *http.Request) {
	nodeHashes := cs.hydfs.GetNodeHashes()
	members := cs.controller.membership.GetAllMembers()
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID.String() < members[j].ID.String()
	})

	for _, member := range members {
		nodeHash := nodeHashes[member.ID.String()]
		fmt.Fprintf(w, "  %s | %s | Inc:%d | RingID: %d\n",
			member.ID, member.Status, member.Incarnation, nodeHash)
	}
}

func (cs *ControlServer) handleListSelf(w http.ResponseWriter, r *http.Request) {
	member := cs.controller.membership.Members[cs.controller.membership.LocalNode.String()]
	fmt.Fprintf(w, "  %s | %s | Inc:%d\n", member.ID, member.Status, member.Incarnation)
}

func (cs *ControlServer) handleDisplaySuspects(w http.ResponseWriter, r *http.Request) {
	suspects := cs.controller.membership.GetSuspectedMembers()
	for _, member := range suspects {
		fmt.Fprintf(w, "  %s | %s | Inc:%d\n", member.ID, member.Status, member.Incarnation)
	}
}

func (cs *ControlServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	intro := q.Get("introducer")
	if intro == "" {
		http.Error(w, "introducer is required", http.StatusBadRequest)
		return
	}
	if err := cs.controller.JoinGroup(intro); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

func (cs *ControlServer) handleLeave(w http.ResponseWriter, r *http.Request) {
	cs.controller.LeaveGroup()
	io.WriteString(w, "ok\n")
}

// --- Client Command Runner ---

// Helper to build a remote base URL
func buildRemoteBaseURL(host string, defaultControlPort int) string {
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, defaultControlPort)
	}
	return fmt.Sprintf("http://%s", host)
}

// **** FIX: Changed signature to accept positional arguments ****
func runClient(cmd string, controlPort int, args []string) {
	localBase := fmt.Sprintf("http://127.0.0.1:%d", controlPort)
	var endpoint string
	var err error

	switch strings.ToLower(cmd) {
	// --- MP2 Commands ---
	case "list_mem":
		endpoint = localBase + "/list_mem"
	case "list_mem_ids":
		endpoint = localBase + "/list_mem_ids"
	case "list_self":
		endpoint = localBase + "/list_self"
	case "display_suspects":
		endpoint = localBase + "/display_suspects"
	case "join":
		if len(args) < 1 {
			log.Fatal("join requires introducer ip:port argument")
		}
		endpoint = localBase + "/join?" + url.Values{"introducer": {args[0]}}.Encode()
	case "leave":
		endpoint = localBase + "/leave"

	// --- MP1 Command ---
	case "grep_logs":
		if len(args) < 1 {
			log.Fatal("grep_logs requires a grep command argument, e.g. \"grep -E 'SUSPECT|FAILED'\"")
		}
		grepCmd := args[0]
		fileType := ""
		if len(args) > 1 {
			fileType = args[1]
		}
		runGrepLogs(grepCmd, fileType)
		return

	// --- MP3 Commands ---
	case "create":
		if len(args) < 2 {
			log.Fatal("create requires arg1=localfilename arg2=HyDFSfilename")
		}
		err = httpPostFile(localBase+"/create", args[0], args[1], false)
	case "append":
		if len(args) < 2 {
			log.Fatal("append requires arg1=localfilename arg2=HyDFSfilename")
		}
		err = httpPostFile(localBase+"/append", args[0], args[1], false)
	case "get":
		if len(args) < 2 {
			log.Fatal("get requires arg1=HyDFSfilename arg2=localfilename")
		}
		err = httpGetFile(localBase+"/get?hydfsfile="+url.QueryEscape(args[0]), args[1])
	case "merge":
		if len(args) < 1 {
			log.Fatal("merge requires arg1=HyDFSfilename")
		}
		endpoint = localBase + "/merge?hydfsfile=" + url.QueryEscape(args[0])
	case "ls":
		if len(args) < 1 {
			log.Fatal("ls requires arg1=HyDFSfilename")
		}
		endpoint = localBase + "/ls?hydfsfile=" + url.QueryEscape(args[0])
	case "liststore":
		endpoint = localBase + "/liststore"

	// --- MP3 Demo Commands ---
	case "getfromreplica":
		if len(args) < 3 {
			log.Fatal("getfromreplica requires arg1=VMaddress arg2=HyDFSfilename arg3=localfilename")
		}
		vmAddress := args[0]
		hydfsFilename := args[1]
		localFilename := args[2]

		remoteBase := buildRemoteBaseURL(vmAddress, controlPort)
		remoteURL := remoteBase + "/get?hydfsfile=" + url.QueryEscape(hydfsFilename)
		log.Printf("Attempting to fetch from remote replica: %s", remoteURL)
		err = httpGetFile(remoteURL, localFilename)

	case "multiappend":
		if len(args) < 3 { // e.g., multiappend file.txt vm1 local1.txt
			log.Fatal("multiappend requires arg1=HyDFSfilename followed by pairs of (VMi localfilei)")
		}
		hydfsFilename := args[0]
		vmFilePairs := args[1:]

		if len(vmFilePairs)%2 != 0 {
			log.Fatal("multiappend requires an even number of arguments for VMaddress and localfilename pairs")
		}

		var wg sync.WaitGroup
		log.Printf("Launching %d simultaneous appends to %s...", len(vmFilePairs)/2, hydfsFilename)

		for i := 0; i < len(vmFilePairs); i += 2 {
			vmAddress := vmFilePairs[i]
			localFilename := vmFilePairs[i+1]

			wg.Add(1)
			go func(vm, lfile string) {
				defer wg.Done()
				remoteBase := buildRemoteBaseURL(vm, controlPort)
				remoteURL := remoteBase + "/append"
				log.Printf("-> Starting append from %s (file: %s) to %s", vm, lfile, hydfsFilename)
				if err := httpPostFile(remoteURL, lfile, hydfsFilename, true); err != nil {
					log.Printf("ERROR from %s: %v", vm, err)
				} else {
					log.Printf("<- Finished append from %s", vm)
				}
			}(vmAddress, localFilename)
		}

		wg.Wait()
		log.Println("All multiappend operations complete.")
		return

	default:
		log.Fatalf("unknown cmd: %s", cmd)
	}

	if err != nil {
		log.Fatalf("Client request failed: %v", err)
	}

	if endpoint == "" {
		return
	}

	resp, err := http.Get(endpoint)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

// runGrepLogs (Helper for client)
func runGrepLogs(grepCmd, fileType string) {
	if fileType == "" {
		fileType = logcommon.DefaultFileType
	}
	hostsFile := os.Getenv("MP2_LOG_HOSTS_FILE")
	if hostsFile == "" {
		hostsFile = logclient.DefaultHostsFile()
	}
	opts := logclient.Options{
		HostsFile: hostsFile,
		Port:      logcommon.DefaultPort,
		Request: logcommon.ServerRequest{
			Input:    grepCmd,
			FileType: fileType,
		},
		Output:    os.Stdout,
		ErrOutput: os.Stderr,
	}
	if portEnv := os.Getenv("MP2_LOG_PORT"); portEnv != "" {
		if p, err := strconv.Atoi(portEnv); err == nil {
			opts.Port = p
		}
	}
	if err := logclient.Run(context.Background(), opts); err != nil {
		log.Fatalf("grep_logs failed: %v", err)
	}
}

// httpPostFile is a client helper for 'create' and 'append'
func httpPostFile(fullURL string, localFilename string, hydfsFilename string, quiet bool) error {
	file, err := os.Open(localFilename)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %w", localFilename, err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("hydfsfile", hydfsFilename); err != nil {
		return err
	}

	part, err := writer.CreateFormFile("localfile", filepath.Base(localFilename))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, file); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, fullURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if !quiet {
		fmt.Println(string(respBody))
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status: %s", resp.Status)
	}
	return nil
}

// httpGetFile is a client helper for 'get'
func httpGetFile(fullURL string, localFilename string) error {
	file, err := os.Create(localFilename)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %w", localFilename, err)
	}
	defer file.Close()

	resp, err := http.Get(fullURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status: %s\nBody: %s", resp.Status, string(respBody))
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	fmt.Printf("File successfully downloaded to %s\n", localFilename)
	return nil
}
