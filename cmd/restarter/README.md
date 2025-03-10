# Restarter

## Usage

```bash
go run cmd/restarter/main.go -path=/path/to/your/application
```

## Description

This is a simple application that will restart a given application if it crashes.

## How it works

1. The application will run and monitor the given application.
2. If the application crashes, the restarter will restart it.
3. If the application exits without errors, the restarter will stop monitoring it.