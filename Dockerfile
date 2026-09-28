FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY clinical_pipeline.py gx_check.py demo.py test_pipeline.py test_gx.py ./
# Containers run with network_mode: none; keep GX from attempting analytics calls.
ENV GX_ANALYTICS_ENABLED=false TQDM_DISABLE=1
RUN useradd --create-home --uid 10001 runner && mkdir -p /app/evidence /app/data && chown -R runner:runner /app
USER runner
CMD ["python", "demo.py", "--output", "/app/evidence/demo.json"]
