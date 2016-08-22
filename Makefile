default: server

server: deps
	mkdir -p bin
	go get github.com/tools/godep && godep restore
	go build -o bin/workq-server cmd/workq-server/*

deps:
	go get github.com/tools/godep && godep restore

test:
	go test -v ./...
