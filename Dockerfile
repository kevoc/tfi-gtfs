
FROM python:3.13

# copy the source code
WORKDIR /app
COPY . /app/

RUN python3 -m pip install .

ENTRYPOINT [ "/app/entrypoint.sh" ]
