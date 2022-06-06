FROM python:3.9

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /consumer_app

COPY . /consumer_app

ENV PATH = /root/.local:$PATH

CMD ["python","./consumer-app.py"]