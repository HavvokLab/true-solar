huawei2:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o huawei2 ./cmd/huawei2/main.go

kstar:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o kstar ./cmd/kstar/main.go

delete_doc:
	env GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external" -o delete_doc ./cmd/delete_doc/main.go