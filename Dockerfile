FROM rust:1.85-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p pontmored

FROM debian:bookworm-slim
RUN useradd --system --uid 7300 --create-home pontmore
COPY --from=build /src/target/release/pontmored /usr/local/bin/pontmored
USER pontmore
EXPOSE 7300
ENTRYPOINT ["pontmored"]
