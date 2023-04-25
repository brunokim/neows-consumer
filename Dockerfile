FROM python:3.10-alpine
WORKDIR /app
RUN apk add --no-cache gcc musl-dev linux-headers libpq-dev
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY main.py main.py
CMD ["python", "main.py"]
