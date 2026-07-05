FROM ubuntu:26.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    g++ \
    cmake \
    make \
    pkg-config \
    git \
    liblz4-dev \
    libssl-dev \
    librdkafka-dev \
    libboost-program-options-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY CMakeLists.txt ./
COPY src/ ./src/
RUN mkdir -p build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
    && make -j$(nproc)

CMD ["/app/build/benchmark_client"]
