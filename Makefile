.PHONY: mockgen mock rapid_test fuzz_test unit_test test cover

all: mockgen mock rapid_test fuzz_test unit_test test cover

mockgen:
	mockgen -source sqsx/interface.go -destination sqsx/mock_interface.go -package sqsx
	mockgen -source cloudwatchx/interface.go -destination cloudwatchx/mock_interface.go -package cloudwatchx
	mockgen -source secretsmanagerx/interface.go -destination secretsmanagerx/mock_interface.go -package secretsmanagerx

mock: mockgen

PKGS?=$$(go list ./... | grep -vE '(examples|cmd/tool|cmd/research|cmd/test)' | tr '\n' ',' | sed 's/,$$//')

rapid_test:
	IS_TEST=true go test -v ./sqsx/publisher -rapid.checks=2_000 -run TestRapid
	IS_TEST=true go test -v ./sqsx/consumer -rapid.checks=2_000 -run TestRapid

fuzz_test:
	IS_TEST=true go test -v ./sqsx/publisher -fuzz=FuzzPublishMessageBatchWithEntry -fuzztime=10s
	IS_TEST=true go test -v ./sqsx/consumer -fuzz=FuzzMessageHandler -fuzztime=10s
	IS_TEST=true go test -v ./sqsx/consumer -fuzz=FuzzReceiveAndProcessMessages -fuzztime=10s
	IS_TEST=true go test -v ./sqsx/consumer -fuzz=FuzzPollingLoop -fuzztime=10s

unit_test:
	IS_TEST=true go test -v -race -coverpkg=$(PKGS) ./... -coverprofile=coverage.out.tmp
	grep -vE "mock_interface.go|mock_result_with_context.go" coverage.out.tmp > coverage.out
	rm coverage.out.tmp

build_example:
	go build ./examples/sqsconsumer
	go build ./examples/sqspublisher

test: build_example unit_test

cover:test
	gocov convert coverage.out | gocov-html -t golang > coverage.html