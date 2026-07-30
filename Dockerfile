ARG PYTHON_IMAGE_TAG=3.14.6-slim
FROM python:${PYTHON_IMAGE_TAG}

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home --uid 10001 appuser

COPY --chown=appuser:appuser . .

USER appuser

CMD ["sleep", "infinity"]
