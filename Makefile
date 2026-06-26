build:
	@cargo build

build-release:
	@cargo build --release --locked --bin riffy

run:
	@cargo run

test:
	@cargo test --all

bench:
	cargo bench --all-features

style-check:
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all -- --check

lint: style-check
	@rustup component add clippy 2> /dev/null
	cargo clippy --all --tests --examples -- -D clippy::all -D warnings

format:
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all
	@rustup component add clippy 2> /dev/null
	cargo clippy --all --tests --examples --fix --allow-dirty --allow-staged