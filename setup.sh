#!/bin/bash

# Update package lists
sudo apt update

# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Install build essentials
sudo apt install -y build-essential

# Install ncdu
sudo apt install -y ncdu

# Install ntop
sudo apt install -y ntop

# Clone the repository
git clone https://github.com/ompurwar/1brc.git

git branch pipeline-with-splitting-processing-producer-condumer
git pull origin pipeline-with-splitting-processing-producer-condumer

# Navigate to the repository directory
cd ./1brc
mkdir ./data

# Build and run the generate binary
cargo run --release --bin generate

# Build and run the row1bChellange binary
cargo run --release --bin row1bChellange
