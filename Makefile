format:
	cargo fmt
check_lint:
	cargo clippy
engine_demo:
	RUST_LOG=info cargo run --bin engine_demo
ts_demo:
	cargo run --bin ts_demo
ts_tui:
	RUST_LOG=info cargo run --release --bin ts_runner_demo DO_NOT_COMMIT/ts_config.yaml --tui
ts_cli_client:
	cargo run --bin ts_cli -- --channel channel1 --venue hyperliquid --passport 1234
engine_runner_tui:
	RUST_LOG=info cargo run --bin engine_runner_demo -- DO_NOT_COMMIT/engine_config.yaml --tui

#prod stuff
prod_ts_tui:
	RUST_LOG=info cargo run --release --bin ts_runner_prod DO_NOT_COMMIT/prod/ts_config_prod.yaml --tui
prod_engine_runner_tui:
	RUST_LOG=info cargo run --release --bin mm_strat -- DO_NOT_COMMIT/prod/engine_config_prod.yaml --tui
