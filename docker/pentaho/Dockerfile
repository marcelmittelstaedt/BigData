FROM ubuntu:18.04

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Install All Necessary APT Packages
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y sudo vim unzip python python3 python3-pip postgresql postgresql-contrib git openjdk-8-jdk ssh pdsh openssh-client openssh-server curl htop  && apt-get clean && pip3 install findspark && pip3 install pyspark && pip3 install pyarrow

# Add Airflow User
RUN useradd -ms /bin/bash pentaho

# Switch To Airflow User
USER pentaho
WORKDIR /home/pentaho

# Install Hadoop, Hive and Spark Client
RUN wget http://archive.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz && tar -xzf hadoop-3.1.2.tar.gz && rm hadoop-3.1.2.tar.gz && mv hadoop-3.1.2 hadoop && wget https://www-eu.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz && tar -xvzf apache-hive-3.1.2-bin.tar.gz && rm apache-hive-3.1.2-bin.tar.gz && mv apache-hive-3.1.2-bin hive && wget https://www-us.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz && tar -xvzf spark-2.3.4-bin-hadoop2.7.tgz && rm spark-2.3.4-bin-hadoop2.7.tgz && mv spark-2.3.4-bin-hadoop2.7 spark

COPY --chown=pentaho .bashrc /home/pentaho/.bashrc
COPY --chown=pentaho core-site.xml hadoop/etc/hadoop/core-site.xml
COPY --chown=pentaho hive-site.xml spark/conf/hive-site.xml
COPY --chown=pentaho hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
COPY --chown=pentaho spark-env.sh /home/pentaho/spark/conf/spark-env.sh
COPY --chown=pentaho yarn-site.xml hadoop/etc/hadoop/yarn-site.xml

# Download and Install Pentaho
RUN wget https://cfhcable.dl.sourceforge.net/project/pentaho/Pentaho%208.0/client-tools/pdi-ce-8.0.0.0-28.zip && mkdir -p /home/pentaho/pentaho && unzip /home/pentaho/pdi-ce-8.0.0.0-28.zip -d /home/pentaho/pentaho && rm /home/pentaho/pdi-ce-8.0.0.0-28.zip 

# Setup kettle.properties and Hive JDBC jar
RUN mkdir /home/pentaho/.kettle && cp /home/pentaho/hive/jdbc/hive-jdbc-3.1.2-standalone.jar /home/pentaho/pentaho/data-integration/lib

# Switch back to Root User
USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

# Start startup Script
ENTRYPOINT ["/startup.sh"]
