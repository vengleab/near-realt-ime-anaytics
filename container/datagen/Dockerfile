FROM python:3.7-slim

RUN apt update -y && apt install gcc libpq-dev -y

RUN pip install \
    psycopg2-binary==2.9.3 \
    faker==13.3.2 \
    psycopg2    


WORKDIR /opt/datagen

COPY ../../datagen /opt/datagen

CMD [ "python" "./gen_user_payment_data.py"]