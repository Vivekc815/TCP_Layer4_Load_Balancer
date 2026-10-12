# Start from Ubuntu Linux, version 24.04
FROM ubuntu:24.04

# Install: C++ compiler, netcat (nc), and network tools (lsof, tc)
RUN apt-get update && apt-get install -y \
    g++ \
    netcat-openbsd \
    lsof \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# The folder inside Linux where your code will live
WORKDIR /app
