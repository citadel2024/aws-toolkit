.PHONY: mockgen mock rapid_test fuzz_test unit_test test cover

all: mockgen mock rapid_test fuzz_test unit_test test cover

mockgen:
	mockgen -source sqs/interface.go -destination sqs/mock_interface.go -package sqs

mock: mockgen

PKGS?=$$(go list ./... | grep -vE '(examples|cmd/tool|cmd/research|cmd/test)' | tr '\n' ',' | sed 's/,$$//')

rapid_test:
	IS_TEST=true go test -v ./sqs/publisher -rapid.checks=2_000 -run TestRapid
	IS_TEST=true go test -v ./sqs/consumer -rapid.checks=2_000 -run TestRapid

fuzz_test:
	IS_TEST=true go test -v ./sqs/publisher -fuzz=FuzzPublishMessageBatchWithEntry -fuzztime=10s
	IS_TEST=true go test -v ./sqs/consumer -fuzz=FuzzMessageHandler -fuzztime=10s
	IS_TEST=true go test -v ./sqs/consumer -fuzz=FuzzReceiveAndProcessMessages -fuzztime=10s
	IS_TEST=true go test -v ./sqs/consumer -fuzz=FuzzPollingLoop -fuzztime=10s

unit_test:
	IS_TEST=true go test -v -race -coverpkg=$(PKGS) ./... -coverprofile=coverage.out.tmp
	grep -vE "mock_interface.go|mock_result_with_context.go" coverage.out.tmp > coverage.out
	rm coverage.out.tmp

test: unit_test

cover:test
	gocov convert coverage.out | gocov-html -t golang > coverage.html