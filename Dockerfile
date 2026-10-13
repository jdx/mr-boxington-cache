FROM rust:1.99-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS builder
ARG MBX_CACHE_BUILD_REVISION=unknown
ENV MBX_CACHE_BUILD_REVISION=$MBX_CACHE_BUILD_REVISION
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --locked --release

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=builder /src/target/release/mbx-cache /usr/local/bin/mbx-cache
ENV MBX_CACHE_DATA_DIR=/tmp/mbx-cache
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/mbx-cache"]
