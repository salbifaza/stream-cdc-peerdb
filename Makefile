.PHONY: help up down reset logs mirror verify status smoke

help:
	@echo "Usage:"
	@echo "  make up              Start all services (PeerDB control plane + source + destination)"
	@echo "  make down            Stop and remove containers (keeps data)"
	@echo "  make reset           Stop, wipe all volumes, and rebuild from scratch"
	@echo "  make logs            Tail all service logs"
	@echo ""
	@echo "  make mirror          Create peers + mirror (idempotent, waits for PeerDB readiness)"
	@echo "  make verify          Row-count check + live insert/update/delete test"
	@echo "  make status          Replication lag, batch history, slot size, per-table counts"
	@echo "  make smoke           Full end-to-end smoke test (mirror + verify)"

# ── infrastructure ────────────────────────────────────────────────────────────

up:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d
	@echo ""
	@echo "Stack starting (11 containers — may take a minute for Temporal to initialize)."
	@echo "  peerdb-ui:     http://localhost:$${PEERDB_UI_PORT:-3001}"
	@echo "  temporal-ui:   http://localhost:8085"
	@echo "  ClickHouse:    localhost:8123 (HTTP), localhost:9000 (native)"
	@echo "  Source PG:     localhost:5432"
	@echo ""
	@echo "Run 'make mirror' once the stack is healthy, then 'make verify' to test CDC."

down:
	docker compose down

reset:
	docker compose down -v
	@echo "All volumes wiped. Run 'make up' to rebuild from scratch."

logs:
	docker compose logs -f

# ── mirror lifecycle ──────────────────────────────────────────────────────────

mirror:
	./scripts/create_mirror.sh

verify:
	./scripts/verify_cdc.sh

status:
	./scripts/mirror_status.sh

# ── testing ───────────────────────────────────────────────────────────────────

smoke: mirror verify
