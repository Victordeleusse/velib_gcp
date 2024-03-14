FROM python:3.8-slim

# GCLOUD set-up
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# Google Cloud SDK config
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y

COPY . /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]

