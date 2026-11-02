# miniwdl 1.16.0's default tools image is amd64-only. This Arm64 equivalent
# provides the same two programs used by its HTTP/FTP and S3 downloaders.
FROM ubuntu:26.04 AS awscli_builder
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y curl unzip ca-certificates
RUN curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip -o /tmp/awscli.zip \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install --install-dir /opt/aws-cli --bin-dir /opt/aws-cli-bin

FROM ubuntu:26.04
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y aria2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=awscli_builder /opt/aws-cli /opt/aws-cli
COPY --from=awscli_builder /opt/aws-cli-bin/ /usr/local/bin/
RUN aria2c --version >/dev/null && aws --version
