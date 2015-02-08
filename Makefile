build:
	go build
	go build main/consume.go

deps:
	go get

test:
	go test -race .

travis: deps build test
