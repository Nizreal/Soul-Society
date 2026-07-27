FROM ubuntu:latest

# Install Rust
RUN apt-get update && apt-get install -y cargo

# Copy everything
COPY . /app
WORKDIR /app

# Build it I guess?
RUN cargo build

# Run the server
CMD ["cargo", "run", "--bin", "server"]
