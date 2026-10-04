.PHONY: up down demo test check logs

up:     ## build and start Postgres and the service on :8080
	docker compose up -d --build --wait telemetry

down:   ## stop everything and drop the database
	docker compose down -v

demo: up  ## walk through write, duplicate, out of order, new event type
	./scripts/demo.sh

test:   ## unit tests plus the Postgres tests for dedup and ordering
	docker compose run --rm go go test ./...

check:  ## the CI guardrail; BASE=main also diffs against a branch and shows review routing
	docker compose run --rm --no-deps go sh -c 'apk add -q git && git config --global --add safe.directory /src && go run ./tools/platformcheck $(if $(BASE),-base $(BASE))'

logs:
	docker compose logs -f telemetry
