# ---- Build Stage ----
FROM hexpm/elixir:1.12.0-erlang-24.0.1-debian-buster-20210326 as build_layer

# Set environment variables for building the application
ENV MIX_ENV=prod \
    LANG=C.UTF-8

RUN apt-get --allow-releaseinfo-change update
RUN apt-get install -y openssl gcc git

# Install hex and rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Create the application build directory
RUN mkdir /app
WORKDIR /app

# Copy over all the necessary application files and directories
COPY config ./config
COPY lib ./lib
COPY priv ./priv
COPY mix.exs .
COPY mix.lock .

# Fetch the application dependencies and build the application
RUN mix deps.get
RUN mix deps.compile
RUN mix release

# ---- Application Stage ----
FROM debian:buster-slim AS app

ENV LANG=C.UTF-8

# Install openssl
RUN apt-get update && apt-get install -y openssl

# Copy over the build artifact from the previous step and create a non root user
RUN useradd --create-home app
WORKDIR /home/app
COPY --from=build_layer /app/_build .
RUN chown -R app: ./prod
USER app

# Run the Phoenix app
CMD ["./prod/rel/pulsar/bin/pulsar", "start"]
