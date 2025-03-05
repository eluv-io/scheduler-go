#!/bin/bash

go generate ./cmd/sched
go build ./cmd/sched
