package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Configuration Constants - probably should base these on SSH configuration
const (
	RemotePort  = "22"
	SocketDir   = ".ssh/sockets"
	IdleTimeout = 5 * time.Minute
)

// Protocol Constants (Packet Types)
const (
	TypeStdout = 0x01
	TypeStderr = 0x02
	TypeExit   = 0x03
)

// Flags
var daemonIdentity = flag.String("daemon", "", "Internal use: run as daemon for specific link identity")
var batchMode = flag.Bool("batch", false, "Run in batch mode (reads commands from stdin)")

func main() {
	flag.Parse()

	userHome, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Error getting home dir: %v", err)
	}

	// 1. No master up, start master
	if *daemonIdentity != "" {
		linkName := *daemonIdentity
		startMaster(resolveHost(linkName), getSocketPath(userHome, linkName), userHome)
		return
	}

	// 2. Run client
	linkName := filepath.Base(os.Args[0])
	socketPath := getSocketPath(userHome, linkName)

	conn, err := connectToMaster(socketPath, linkName)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// 2A. Run batch mode
	if *batchMode {
		runBatchMode(conn)
		return
	}

	// 2B. Run simple mode
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

func getSocketPath(homeDir, linkName string) string {
	return filepath.Join(homeDir, SocketDir, fmt.Sprintf("%s.sock", linkName))
}

func resolveHost(linkName string) string {
	if strings.Contains(linkName, "mcpi") {
		return "mcpi"
	}
	if strings.Contains(linkName, "ftb") {
		return "ftb"
	}
	return linkName
}
