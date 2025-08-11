.PHONY: mockgen mock rapid_test fuzz_test unit_test test cover

all: mockgen mock rapid_test fuzz_test unit_test test cover

mockgen:
	mockgen -source sqs/interface.go -destination sqs/mock_interface.go -package sqs

mock: mockgen

PKGS?=$$(go list ./... | grep -vE '(build|cmd/tool|cmd/research|cmd/test)' | tr '\n' ',' | sed 's/,$$//')

rapid_test:
	IS_TEST=true go test -v ./... -rapid.checks=20_000 -run TestRapid

fuzz_test:
	IS_TEST=true go test -v ./sqs/publisher -fuzz=Fuzz -fuzztime=10s

unit_test:
	IS_TEST=true go test -v -race -coverpkg=$(PKGS) ./... -coverprofile=coverage.out.tmp
	grep -vE "mock_cwapi.go|mock_sqsapi.go|mock_interface.go|wire_gen.go|mock_driver_with_context.go|mock_session_with_context.go|mock_result_with_context.go|grule/rules.go|grule/rules_gen.go" coverage.out.tmp > coverage.out
	rm coverage.out.tmp

test: rapid_test fuzz_test unit_test

cover:test
	gocov convert coverage.out | gocov-html -t golang > coverage.html