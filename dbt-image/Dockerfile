FROM python:3.12-slim

# dbt-sqlserver talks to SQL Server through Microsoft's ODBC Driver 18.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates git \
 && curl -sSL -o /tmp/mprod.deb https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb \
 && dpkg -i /tmp/mprod.deb && rm /tmp/mprod.deb \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r /tmp/requirements.txt

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
# Bake the dbt project into the image so Airflow's DockerOperator can run it without a volume.
COPY ny_taxi .

CMD ["dbt", "debug"]
