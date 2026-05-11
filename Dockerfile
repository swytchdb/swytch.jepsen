FROM debian:trixie

ENV DEBIAN_FRONTEND=noninteractive \
    LEIN_ROOT=true \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin

# Base packages used by both roles (sshd worker and lein controller).
# The jepsen.swytch.os layer apt-installs its own dependencies onto the
# worker at test-setup time, so we only need a working apt + sshd here.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        wget \
        gnupg \
        git \
        sudo \
        openssh-server \
        openssh-client \
        iproute2 \
        iptables \
        procps \
        rsync \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Controller-role tooling (JDK + leiningen + go). Worker pods don't
# invoke these but a single image keeps deploys simple.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ARG LEIN_VERSION=2.11.2
RUN curl -fsSLo /usr/local/bin/lein \
        "https://raw.githubusercontent.com/technomancy/leiningen/${LEIN_VERSION}/bin/lein" \
    && chmod +x /usr/local/bin/lein \
    && lein version

ARG GO_VERSION=1.23.4
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
        -o /tmp/go.tar.gz \
    && tar -C /usr/local -xzf /tmp/go.tar.gz \
    && rm /tmp/go.tar.gz \
    && go version

# sshd config: root login with key, no passwords. Authorized keys are
# mounted at /etc/jepsen/keys/authorized_keys by the workflow (Secret).
RUN mkdir -p /run/sshd /root/.ssh /etc/jepsen/keys \
    && chmod 700 /root/.ssh \
    && sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# Pre-populate the controller workspace with the jepsen source so the
# controller pod can run lein without a network checkout. The workflow
# can override the ref at runtime via JEPSEN_REF.
WORKDIR /opt/jepsen
COPY . /opt/jepsen
RUN lein deps

COPY .github/scripts/run-sshd /usr/local/bin/run-sshd
COPY .github/scripts/run-jepsen /usr/local/bin/run-jepsen
RUN chmod +x /usr/local/bin/run-sshd /usr/local/bin/run-jepsen

EXPOSE 22
