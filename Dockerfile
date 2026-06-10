FROM python:3.12-slim
RUN pip install --no-cache-dir requests pyyaml
COPY downloader.py /app/downloader.py
WORKDIR /app
ENTRYPOINT ["python3", "/app/downloader.py"]
CMD ["--loop"]
