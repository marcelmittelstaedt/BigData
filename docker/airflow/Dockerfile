FROM ubuntu:18.04

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Install All Necessary APT Packages
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y sudo vim python3 python3-pip postgresql postgresql-contrib && apt-get clean

# Copy systemd config files for airflow
#COPY airflow-scheduler.service /etc/systemd/system/airflow-scheduler.service
#COPY airflow-webservice.service /etc/systemd/system/airflow-webservice.service

# Copy config files for PostgreSQL 
COPY postgresql.conf /etc/postgresql/10/main/postgresql.conf
COPY pg_hba.conf /etc/postgresql/10/main/pg_hba.conf
RUN sudo chmod -v 744 /etc/postgresql/10/main/postgresql.conf && sudo chmod -v 744 /etc/postgresql/10/main/pg_hba.conf

# Add Airflow User
RUN useradd -ms /bin/bash airflow

# Switch To Airflow User
USER airflow
WORKDIR /home/airflow

COPY --chown=airflow .bashrc /home/airflow/.bashrc

# Install Airflow
RUN pip3 install --user apache-airflow[postgres,gcp_api] gunicorn

# Copy Airflow Config
COPY --chown=airflow airflow.cfg /home/airflow/airflow/airflow.cfg

# Switch back to Root User
USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

# Expose Airflow Web Service Port
EXPOSE 8080

# Start startup Script
ENTRYPOINT ["/startup.sh"]
