FROM python:3.10-slim-bullseye
WORKDIR /app
COPY . /app

RUN apt-get update -y && apt-get install -y awscli

RUN pip install -r requirements.txt
CMD ["python3", "app.py"]