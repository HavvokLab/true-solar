package main

import (
	"flag"
	"log"
	"time"
)

// parseFlags parses the workerPoolSize, startDate, endDate, and vendor flags and returns them.
func parseFlags() (time.Time, time.Time, string) {
	// Define flags
	workerPoolSize := flag.Int("workerPoolSize", 5, "Worker pool size")
	startDateStr := flag.String("startDate", "", "Start date in format YYYY-MM-DD")
	endDateStr := flag.String("endDate", "", "End date in format YYYY-MM-DD")
	vendor := flag.String("vendor", "", "Vendor name")

	// Parse flags
	flag.Parse()

	// Validate input
	if *startDateStr == "" || *endDateStr == "" {
		log.Fatal("Both startDate and endDate must be provided.")
	}

	// Parse dates
	startDate, err := time.Parse("2006-01-02", *startDateStr)
	if err != nil {
		log.Fatalf("Invalid startDate format: %v", err)
	}

	endDate, err := time.Parse("2006-01-02", *endDateStr)
	if err != nil {
		log.Fatalf("Invalid endDate format: %v", err)
	}

	// Check if startDate is before endDate
	if !startDate.Before(endDate) {
		log.Fatal("startDate must be before endDate.")
	}

	if workerPoolSize != nil {
		WorkerPoolSize = *workerPoolSize
	}

	return startDate, endDate, *vendor
}
