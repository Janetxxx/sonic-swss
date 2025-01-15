build:
	cargo build

run:
	cd ./services && docker compose up -d
	RUST_LOG=info cargo run
	curl http://localhost:8889/metrics

debug:
	cd ./services && docker compose up -d
	RUST_LOG=trace cargo run
	curl http://localhost:8889/metrics