FROM python:3.12-alpine

# Minimal deps — no build tools needed for pure-python + requests
RUN pip install --no-cache-dir requests==2.32.3

WORKDIR /app
COPY common.py crawler.py query.py convert.py normalize.py ./

VOLUME ["/data"]

CMD ["python", "-u", "crawler.py"]
