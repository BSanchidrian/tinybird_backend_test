FROM python:3.11

WORKDIR /app

COPY app.py .
COPY setup.py .

RUN pip install -e .

RUN mkdir /app/csv

CMD ["tiny_data_server"]