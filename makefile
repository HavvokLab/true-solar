huawei2:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o huawei2 ./cmd/huawei2/main.go

kstar:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o kstar ./cmd/kstar/main.go

solarman:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o solarman ./cmd/solarman/main.go

growatt:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o growatt ./cmd/growatt/main.go

troubleshoot:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o troubleshoot ./cmd/troubleshoot/main.go

delete_doc:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o delete_doc ./cmd/delete_doc/main.go