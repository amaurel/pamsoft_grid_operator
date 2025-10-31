# Build stage - install dependencies
FROM tercen/pamsoft_grid:2.0.1 AS builder

RUN LD_LIBRARY_PATH="" apt-get -q update && \
    LD_LIBRARY_PATH="" apt-get install -q -y --no-install-recommends \
      curl \
      ca-certificates \
      python3.9 \
      python3.9-dev \
      python3.9-venv \
      git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv

# Create virtual environment
RUN python3.9 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /operator

# Copy project files
COPY pyproject.toml .
COPY main_python.py .

# Install dependencies using uv
RUN uv pip install --python /opt/venv/bin/python .

# Runtime stage - minimal image
FROM tercen/pamsoft_grid:2.0.1

RUN LD_LIBRARY_PATH="" apt-get -q update && \
    LD_LIBRARY_PATH="" apt-get install -q -y --no-install-recommends \
      libtiff-dev \
      python3.9 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /operator

# Copy operator code
COPY main_python.py .

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH"
ENV MCR_PATH=/opt/mcr/v99
ENV LD_LIBRARY_PATH=/opt/mcr/v99/runtime/glnxa64:/opt/mcr/v99/bin/glnxa64:/opt/mcr/v99/sys/os/glnxa64:$LD_LIBRARY_PATH

# Entry point
ENTRYPOINT ["python3.9", "main_python.py"]
