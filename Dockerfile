FROM python:3.12-slim
RUN pip install --no-cache-dir requests pyyaml
COPY downloader.py /app/downloader.py
WORKDIR /app
# No ENTRYPOINT: callers (docker run, compose, signalk-container runJob)
# pass the full command, e.g. ["python3", "/app/downloader.py", "--loop"]
CMD ["python3", "/app/downloader.py", "--loop"]
