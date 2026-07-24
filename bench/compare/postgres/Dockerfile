FROM elixir:1.18.1-otp-27

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      git \
      libpq-dev \
      python3 \
      python3-pip \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/docket-bench-venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV HEX_CACERTS_PATH=/etc/ssl/certs/ca-certificates.crt

RUN python3 -m venv "${VIRTUAL_ENV}"

WORKDIR /work

COPY bench/compare/requirements.txt /tmp/docket-bench-requirements.txt
RUN pip install --no-cache-dir -r /tmp/docket-bench-requirements.txt

RUN mix archive.install github hexpm/hex tag v2.4.2 --force \
    && install -d -m 755 /root/.mix \
    && curl -fsSL \
      https://github.com/erlang/rebar3/releases/download/3.24.0/rebar3 \
      -o /root/.mix/rebar3 \
    && chmod 755 /root/.mix/rebar3

ENV MIX_REBAR3=/root/.mix/rebar3

COPY mix.exs mix.lock ./
RUN MIX_ENV=prod mix deps.get \
    && MIX_ENV=prod mix deps.compile

COPY . .
RUN MIX_ENV=prod mix compile

CMD ["python", "bench/compare/postgres/suite.py"]
