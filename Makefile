default: server

server: deps
	mkdir -p bin
	go get github.com/tools/godep && godep restore
	go build -o bin/workq-server cmd/workq-server/*.go

deps:
	go get github.com/tools/godep && godep restore

test:
	go test -v -p 1 -race ./int/... && go test -v ./cmd/workq-server/systests/...
