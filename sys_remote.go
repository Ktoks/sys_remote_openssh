package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic" // <--- NEW: Required for thread-safe counters
	"syscall"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Configuration Constants
const (
	RemotePort  = "22"
	SocketDir   = ".ssh/sockets"
	IdleTimeout = 5 * time.Minute
)

// OutputPacket defines the protocol between Master and Client
type OutputPacket struct {
	IsStderr bool
	IsExit   bool
	ExitCode int
	Data     []byte
}

// Thread-safe encoder to prevent race conditions between Stdout and Stderr
type SafeEncoder struct {
	mu  sync.Mutex
	enc *gob.Encoder
}

func (s *SafeEncoder) Encode(pkt OutputPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(pkt)
}

// Flags
// We use a string flag. If empty, we are client. If set, we are daemon for that specific identity.
var daemonIdentity = flag.String("daemon", "", "Internal use: run as daemon for specific link identity")
var batchMode = flag.Bool("batch", false, "Run in batch mode (reads commands from stdin)")

func main() {
	flag.Parse()

	userHome, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Error getting home dir: %v", err)
	}

	// 1. MASTER MODE
	if *daemonIdentity != "" {
		linkName := *daemonIdentity
		runMaster(resolveHost(linkName), getSocketPath(userHome, linkName), userHome)
		return
	}

	// 2. CLIENT MODE
	linkName := filepath.Base(os.Args[0])
	socketPath := getSocketPath(userHome, linkName)

	conn, err := connectToMaster(socketPath, linkName)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// // --- NEW: BATCH MODE ---
	// // Usage: echo "ls -la" | ./localhost --batch
	// if len(os.Args) > 1 && os.Args[1] == "--batch" {
	// 	runBatchMode(conn)
	// 	return
	// }
	// // -----------------------

	// // Standard Single Command Mode
	// args := os.Args[1:]
	// if len(args) == 0 {
	// 	log.Fatal("Usage: <symlink> <cmd> OR <symlink> --batch")
	// }
	// remoteCmd := strings.Join(args, " ")

	// if err := sendCommand(conn, remoteCmd); err != nil {
	// 	log.Fatal(err)
	// }
	// A. BATCH MODE (High Performance)
	// --------------------------------
	// Triggered by the newly defined flag
	if *batchMode {
		runBatchMode(conn)
		return
	}

	// B. STANDARD MODE (Single Command)
	// ---------------------------------
	// We use flag.Args() which contains everything AFTER the flags.
	// e.g. "./localhost ls -la" -> flag.Args() is ["ls", "-la"]
	args := flag.Args()
	if len(args) == 0 {
		fmt.Printf("Usage: %s <command> OR %s --batch\n", linkName, linkName)
		os.Exit(1)
	}

	remoteCmd := strings.Join(args, " ")

	if err := sendCommand(conn, remoteCmd); err != nil {
		log.Fatal(err)
	}
}

// NEW FUNCTION: Handles persistent connection for multiple commands
func runBatchMode(conn net.Conn) {
	// 1. Sender Routine (Async)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			cmd := scanner.Text()
			if strings.TrimSpace(cmd) == "" {
				continue
			}

			// Write command to socket
			_, err := fmt.Fprintf(conn, "%s\n", cmd)
			if err != nil {
				return
			}
		}

		// --- CRITICAL FIX: Signal EOF to Master ---
		// We close the *Write* end of the connection.
		// This tells Master: "No more commands coming," but keeps the *Read* end open
		// so we can still receive the results of the commands currently running.
		if unixConn, ok := conn.(*net.UnixConn); ok {
			unixConn.CloseWrite()
		} else if tcpConn, ok := conn.(*net.TCPConn); ok {
			// Fallback if you ever switch to TCP
			tcpConn.CloseWrite()
		}
	}()

	// 2. Receiver Routine (Main Thread)
	dec := gob.NewDecoder(conn)
	for {
		var pkt OutputPacket
		if err := dec.Decode(&pkt); err != nil {
			if err == io.EOF {
				// Success! Master finished all jobs and closed connection.
				break
			}
			log.Printf("Protocol error: %v", err)
			break
		}

		if pkt.IsExit {
			if pkt.ExitCode != 0 {
				fmt.Fprintf(os.Stderr, "[Remote Exit %d]\n", pkt.ExitCode)
			}
			continue
		}

		if pkt.IsStderr {
			os.Stderr.Write(pkt.Data)
		} else {
			os.Stdout.Write(pkt.Data)
		}
	}
}

// Refactored helper to clean up main()
func connectToMaster(socketPath, linkName string) (net.Conn, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		if err := startMasterProcess(linkName); err != nil {
			return nil, fmt.Errorf("failed to spawn master: %v", err)
		}
		for i := 0; i < 20; i++ {
			time.Sleep(200 * time.Millisecond)
			conn, err = net.Dial("unix", socketPath)
			if err == nil {
				return conn, nil
			}
		}
		return nil, fmt.Errorf("timeout waiting for master")
	}
	return conn, nil
}

func sendCommand(conn net.Conn, cmd string) error {
	// Same logic as before, just extracted
	_, err := fmt.Fprintf(conn, "%s\n", cmd)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(conn)
	for {
		var pkt OutputPacket
		if err := dec.Decode(&pkt); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if pkt.IsExit {
			os.Exit(pkt.ExitCode)
		}
		if pkt.IsStderr {
			os.Stderr.Write(pkt.Data)
		} else {
			os.Stdout.Write(pkt.Data)
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// MASTER LOGIC
// -----------------------------------------------------------------------------

func runMaster(host string, socketPath string, homeDir string) {
	// Redirect logs to Stderr (captured in log file)
	log.SetOutput(os.Stderr)
	log.Printf("Daemon starting for host: %s", host)

	// 1. Create Socket Directory
	socketDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		log.Fatalf("Daemon failed to create socket dir: %v", err)
	}

	// 2. Setup SSH Connection
	currentUser := os.Getenv("USER")
	client, err := createSSHClient(host, homeDir, currentUser)
	if err != nil {
		log.Fatalf("SSH Handshake Failed: %v", err)
	}
	defer client.Close()
	log.Println("SSH Connection Established.")

	// 3. Listen on Unix Socket
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Daemon failed to listen on socket: %v", err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	// 4. Smart Accept Loop
	// We use an atomic counter to track how many clients are currently connected.
	var activeConnections int32

	for {
		// Set the "Alarm" for 5 minutes from NOW
		listener.(*net.UnixListener).SetDeadline(time.Now().Add(IdleTimeout))

		conn, err := listener.Accept()
		if err != nil {
			// Check if the error is strictly a Timeout
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {

				// TIMEOUT HANDLER:
				// If we have active connections, we are NOT idle. We snooze.
				if atomic.LoadInt32(&activeConnections) > 0 {
					// Log kept minimal to avoid flooding logs on long jobs
					continue
				}

				// If count is 0, we have been truly idle for 5 minutes.
				log.Println("Idle timeout reached (0 active connections). Shutting down.")
				return
			}

			// If it's a real error (socket died), we must exit
			log.Printf("Accept error: %v", err)
			return
		}

		// Connection Accepted: Increment Counter
		atomic.AddInt32(&activeConnections, 1)

		go func() {
			// Ensure we Decrement Counter when this client finishes
			defer atomic.AddInt32(&activeConnections, -1)
			handleRequest(conn, client)
		}()
	}
}

// -----------------------------------------------------------------------------
// MASTER LOGIC UPDATES
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// ROBUST MASTER LOGIC (With Timeouts & Dynamic Limits)
// -----------------------------------------------------------------------------

func handleRequest(conn net.Conn, client *ssh.Client) {
	defer conn.Close()

	safeEnc := &SafeEncoder{
		enc: gob.NewEncoder(conn),
	}
	reader := bufio.NewReader(conn)

	// High concurrency for speed
	maxConcurrency := 50
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for {
		cmdStr, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		cmdStr = strings.TrimSpace(cmdStr)
		if cmdStr == "" {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(cmd string) {
			defer wg.Done()
			defer func() { <-sem }()

			// OPTIMIZATION: Removed context creation overhead
			runRemoteCommand(client, cmd, safeEnc)
		}(cmdStr)
	}

	wg.Wait()
}

func runRemoteCommand(client *ssh.Client, cmdStr string, safeEnc *SafeEncoder) {
	// Optimization: Skip Context/Select overhead if raw speed is priority
	session, err := client.NewSession()
	if err != nil {
		// If we hit the limit, just return quickly
		return
	}

	// Optimization: Combine Output (CombinedOutput is faster than streaming 2 pipes)
	output, err := session.CombinedOutput(cmdStr)
	session.Close() // Close immediately

	// Send result in one big packet (Less syscalls)
	if len(output) > 0 {
		safeEnc.Encode(OutputPacket{IsStderr: false, Data: output})
	}

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*ssh.ExitError); ok {
			exitCode = exitErr.ExitStatus()
		} else {
			exitCode = 1
		}
	}
	safeEnc.Encode(OutputPacket{IsExit: true, ExitCode: exitCode})
}

// -----------------------------------------------------------------------------
// HELPERS
// -----------------------------------------------------------------------------

func getSocketPath(homeDir, linkName string) string {
	// Generates: /home/user/.ssh/sockets/mcpi.sock
	return filepath.Join(homeDir, SocketDir, fmt.Sprintf("%s.sock", linkName))
}

func startMasterProcess(identity string) error {
	selfExe, err := os.Executable()
	if err != nil {
		return err
	}

	homeDir, _ := os.UserHomeDir()
	socketDir := filepath.Join(homeDir, SocketDir)

	// Ensure dir exists
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		return fmt.Errorf("failed to create socket dir: %v", err)
	}

	// Setup Log
	logPath := filepath.Join(socketDir, identity+".log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to create daemon log: %v", err)
	}

	// Spawn Daemon
	cmd := exec.Command(selfExe, "--daemon", identity)

	// CRITICAL FIX: Pass current environment (SSH_AUTH_SOCK) to the daemon
	cmd.Env = os.Environ()

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Detach
	}

	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}

	logFile.Close()
	return nil
}

// func sendPacket(w io.Writer, pkt OutputPacket) {
// 	// In a high-throughput scenario, cache the encoder or use a buffered writer
// 	gob.NewEncoder(w).Encode(pkt)
// }

// func sendError(w io.Writer, msg string) {
// 	sendPacket(w, OutputPacket{IsStderr: true, Data: []byte(msg + "\n")})
// 	sendPacket(w, OutputPacket{IsExit: true, ExitCode: 1})
// }

func createSSHClient(host string, home string, user string) (*ssh.Client, error) {
	// 1. Host Key Verification (Strict)
	khPath := filepath.Join(home, ".ssh", "known_hosts")
	hkCallback, err := knownhosts.New(khPath)
	if err != nil {
		log.Printf("Warning: known_hosts not found or invalid, using insecure fallback for: %s", host)
		hkCallback = ssh.InsecureIgnoreHostKey()
	}

	var auths []ssh.AuthMethod

	// 2. Method A: SSH Agent
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		if conn, err := net.Dial("unix", sock); err == nil {
			agentClient := agent.NewClient(conn)
			// Check if agent actually has keys
			if signers, _ := agentClient.Signers(); len(signers) > 0 {
				auths = append(auths, ssh.PublicKeysCallback(agentClient.Signers))
				log.Println("Added SSH Agent to auth methods")
			}
		}
	}

	// 3. Method B: Private Key Files (Fallback)
	keyFiles := []string{"id_ed25519", "id_rsa"} // Check both common types

	for _, name := range keyFiles {
		keyPath := filepath.Join(home, ".ssh", name)
		keyBytes, err := os.ReadFile(keyPath)
		if err == nil {
			signer, err := ssh.ParsePrivateKey(keyBytes)
			if err == nil {
				auths = append(auths, ssh.PublicKeys(signer))
				log.Printf("Added key file %s to auth methods", name)
			}
		}
	}

	if len(auths) == 0 {
		return nil, fmt.Errorf("no auth methods found (checked Agent, id_ed25519, id_rsa)")
	}

	config := &ssh.ClientConfig{
		User:            user,
		Auth:            auths,
		HostKeyCallback: hkCallback,
		Timeout:         5 * time.Second,
	}

	addr := net.JoinHostPort(host, RemotePort)
	return ssh.Dial("tcp", addr, config)
}

func resolveHost(linkName string) string {
	// 1. Map specific symlinks to specific hostnames
	if strings.Contains(linkName, "mcpi") {
		return "mcpi"
	}
	if strings.Contains(linkName, "ftb") {
		return "ftb"
	}

	// 2. Dynamic Fallback:
	// If the link name is a valid hostname (like "localhost"), use it directly.
	// You might want to add a validation regex here in production.
	return linkName
}
