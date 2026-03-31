FROM python:3.12-alpine

# Minimal deps — no build tools needed for pure-python + requests
RUN pip install --no-cache-dir requests==2.32.3

WORKDIR /app
COPY crawler.py query.py ./

VOLUME ["/data"]

CMD ["python", "-u", "crawler.py"]
