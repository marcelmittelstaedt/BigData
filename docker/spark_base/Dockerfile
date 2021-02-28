FROM marcelmittelstaedt/hiveserver_base:latest

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Install Jupyter
RUN apt-get update && apt-get install -y python git python3 python3-pip && apt-get clean && pip3 install jupyter && pip3 install findspark && pip3 install matplotlib && pip3 install pandas

# Switch To Hadoop User
USER hadoop
WORKDIR /home/hadoop

# Download and Install Hadoop
RUN wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz && tar -xvzf spark-2.3.4-bin-hadoop2.7.tgz && rm spark-2.3.4-bin-hadoop2.7.tgz && mv spark-2.3.4-bin-hadoop2.7 spark

# Install Config Files
COPY .bashrc .bashrc
COPY spark-env.sh /home/hadoop/spark/conf/spark-env.sh
COPY slaves /home/hadoop/spark/conf/slaves
COPY yarn-site.xml hadoop/etc/hadoop/yarn-site.xml
COPY core-site.xml hadoop/etc/hadoop/core-site.xml
COPY hive-site.xml /home/hadoop/spark/conf/hive-site.xml

RUN mkdir /home/hadoop/.jupyter/ 
COPY jupyter_notebook_config.py /home/hadoop/.jupyter/jupyter_notebook_config.py 

# Switch back to Root User
USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

#Expose Hadoop Ports
EXPOSE 8088
EXPOSE 9870
EXPOSE 9000
EXPOSE 10000
EXPOSE 4040
EXPOSE 8042
EXPOSE 8030
EXPOSE 8031
EXPOSE 8032
EXPOSE 8888
EXPOSE 30000-50000

# Start startup Script
ENTRYPOINT ["/startup.sh"]


