FROM marcelmittelstaedt/hadoop_base:latest

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Switch To Hadoop User
USER hadoop
WORKDIR /home/hadoop

# Download and Install Hadoop
RUN wget https://www-eu.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz && tar -xvzf apache-hive-3.1.2-bin.tar.gz && rm apache-hive-3.1.2-bin.tar.gz && mv apache-hive-3.1.2-bin hive

# Install Config Files
COPY .bashrc .bashrc
COPY hive-env.sh /home/hadoop/hive/conf/hive-env.sh
COPY hive-site.xml /home/hadoop/hive/conf/hive-site.xml
COPY mapred-site.xml hadoop/etc/hadoop/mapred-site.xml

# Switch back to Root User
USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

#Expose Hadoop Ports
EXPOSE 8088
EXPOSE 9870

# Start startup Script
ENTRYPOINT ["/startup.sh"]


