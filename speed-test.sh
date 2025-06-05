
#!/bin/bash

set -e

commands=(
  "./target/release/rff-basic-file-write baseline.txt"
  "./target/release/rff-basic-file-write dio.txt --mode dio"
  "./target/release/rff-basic-file-write uring1.txt --mode uring1"
  "./target/release/rff-basic-file-write uring2.txt --mode uring2"
  "./target/release/rff-basic-file-write uring3.txt --mode uring3"

)

for cmd in "${commands[@]}"; do
  echo "Running: $cmd"
  for i in {1..5}; do
    echo "  Round $i..."
    eval "$cmd"
    rm *.txt
  done
  echo
done