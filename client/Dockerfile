FROM ubuntu:26.04@sha256:b7f48194d4d8b763a478a621cdc81c27be222ba2206ca3ca6bc42b49685f3d9e

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
