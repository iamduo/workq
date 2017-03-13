#!/bin/bash

# Combine coverage, from https://github.com/golang/go/issues/6909?__hstc=45444411.c34ab7bf88e972fdd7a7debc8575bac5.1476576000071.1476576000072.1476576000073.1&__hssc=45444411.1.1476576000074&__hsfp=1773666937
export PKGS=$(go list ./... | grep -v -e "/vendor/")
export PKGS_DELIM=$(echo "$PKGS" | paste -sd "," -)
go list -f '{{if or (len .TestGoFiles) (len .XTestGoFiles)}}go test -v -p 1 -coverprofile {{.Name}}_{{len .Imports}}_{{len .Deps}}.coverprofile {{.ImportPath}}{{end}}' $PKGS | xargs -I {} bash -c {} &&
gocovmerge `ls *.coverprofile` > coverage.out &&
goveralls -coverprofile=coverage.out -service=travis-ci
