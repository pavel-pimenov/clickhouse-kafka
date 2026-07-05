FROM ubuntu:26.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    g++ \
    cmake \
    ninja-build \
    ccache \
    pkg-config \
    git \
    liblz4-dev \
    libssl-dev \
    librdkafka-dev \
    libboost-program-options-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CCACHE_DIR=/cache/ccache

WORKDIR /app
COPY CMakeLists.txt ./
COPY src/ ./src/
RUN mkdir -p build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
                -DCMAKE_C_COMPILER_LAUNCHER=ccache \
                -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
                -G Ninja \
    && ninja

CMD ["/app/build/benchmark_client"]
