package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Define the flag for the application path
	appPath := flag.String("path", "", "Path to the application to run and monitor")
	flag.Parse()

	// Validate the provided application path
	if *appPath == "" {
		log.Fatal("You must provide a valid path to the application using the -path flag.")
	}

	// Setting up a channel to listen for signals (SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Channel to control the restart loop
	restartLoop := make(chan bool, 1)

	go func() {
		for {
			select {
			case <-restartLoop:
				// Exit the goroutine if restart loop is stopped
				return
			default:
				if err := runApp(*appPath); err != nil {
					// Log the error and restart the application after a delay
					log.Printf("Application crashed with error: %s. Restarting in 5 seconds...", err)
					time.Sleep(5 * time.Second)
				} else {
					// If the application exits without errors, stop the restart loop
					restartLoop <- false
					return
				}
			}
		}
	}()

	// Block until a signal is received
	<-sigChan
	log.Println("Signal received, stopping application...")
	// Signal the restart loop to stop
	restartLoop <- false
}

func runApp(appPath string) error {
	// Start the application using the path provided in the flag
	cmd := exec.Command(appPath)

	// Start the application
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start the application: %w", err)
	}

	// Wait for the application to exit
	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The application exited with a non-zero status
			return fmt.Errorf("application exited with error: %s", exiterr)
		}
		return fmt.Errorf("application wait failed: %w", err)
	}

	// No error, application exited normally
	return nil
}
