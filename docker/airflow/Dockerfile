FROM ubuntu:18.04

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Install All Necessary APT Packages
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
	&& apt-get install -y sudo vim python python3 python3-pip postgresql postgresql-contrib git openjdk-8-jdk ssh pdsh openssh-client openssh-server curl htop \
	&& apt-get clean \
	&& pip3 install 'findspark==1.3.0' \
	&& pip3 install 'pyspark==2.4.4' 

# Copy config files for PostgreSQL 
COPY postgresql.conf /etc/postgresql/10/main/postgresql.conf
COPY pg_hba.conf /etc/postgresql/10/main/pg_hba.conf
RUN sudo chmod -v 744 /etc/postgresql/10/main/postgresql.conf && sudo chmod -v 744 /etc/postgresql/10/main/pg_hba.conf

# Add Airflow User
RUN useradd -ms /bin/bash airflow

# Switch To Airflow User
USER airflow
WORKDIR /home/airflow

# Install Hadoop, Hive and Spark Client
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz \
	&& tar -xzf hadoop-3.1.2.tar.gz \
	&& rm hadoop-3.1.2.tar.gz \
	&& mv hadoop-3.1.2 hadoop \
	&& wget https://www-eu.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz \
	&& tar -xvzf apache-hive-3.1.2-bin.tar.gz \
	&& rm apache-hive-3.1.2-bin.tar.gz \
	&& mv apache-hive-3.1.2-bin hive \
	&& wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz \
	&& tar -xvzf spark-2.3.4-bin-hadoop2.7.tgz \
	&& rm spark-2.3.4-bin-hadoop2.7.tgz \
	&& mv spark-2.3.4-bin-hadoop2.7 spark

COPY --chown=airflow .bashrc /home/airflow/.bashrc
COPY --chown=airflow core-site.xml hadoop/etc/hadoop/core-site.xml
COPY --chown=airflow hive-site.xml spark/conf/hive-site.xml
COPY --chown=airflow hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
COPY --chown=airflow spark-env.sh /home/airflow/spark/conf/spark-env.sh
COPY --chown=airflow yarn-site.xml hadoop/etc/hadoop/yarn-site.xml

# Install Airflow
RUN pip3 install --upgrade pip \
	&& pip3 install --user 'apache-airflow[postgres,gcp_api]==1.10.6' \
	&& pip3 install --user 'gunicorn==19.9.0' \
	&& pip3 install --user 'werkzeug==0.16.0' \
	&& pip3 install --user 'SQLAlchemy==1.3.15' \
	&& pip3 install --user 'pyarrow==0.11.1' 

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
