FROM python:3.10.12
WORKDIR /blockchain-data-subnet-indexer-bitcoin
COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y \
    python3-dev \
    cmake \
    make \
    gcc \
    g++ \
    libssl-dev

RUN pip install pymgclient
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x scripts/*

