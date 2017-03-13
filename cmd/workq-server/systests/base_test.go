package main_test

import (
	"fmt"
	"os/exec"
	"path/filepath"
)

func serverAddr() string {
	return "localhost:9944"
}

func serverPath() string {
	serverPath, _ := filepath.Abs("./tmp/workq-server")
	return serverPath
}

func buildServer() {
	sourcePath, _ := filepath.Abs("../*.go")
	buildStr := fmt.Sprintf("env go build -o %s %s", serverPath(), sourcePath)
	// sh -c workaround for now, unknown escaping issues with standard Command("go", ...)
	fmt.Printf("%s", buildStr)
	cmd := exec.Command("sh", "-c", buildStr)
	err := cmd.Run()
	if err != nil {
		panic(fmt.Sprintf("Unable to build server, err=%s", err))
	}
}
