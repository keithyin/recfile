build:
	cargo build --release

install:
	cp target/release/bam-rff-cvt /usr/bin
	cp target/release/rff-basic-file-write /usr/bin