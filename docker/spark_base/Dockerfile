FROM marcelmittelstaedt/hiveserver_base:latest

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Install Jupyter
RUN apt-get install -y python3 python3-pip && apt-get clean && pip3 install jupyter && pip3 install findspark

# Switch To Hadoop User
USER hadoop
WORKDIR /home/hadoop

# Download and Install Hadoop
RUN wget https://www-us.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz && tar -xvzf spark-2.3.4-bin-hadoop2.7.tgz && rm spark-2.3.4-bin-hadoop2.7.tgz && mv spark-2.3.4-bin-hadoop2.7 spark

# Install Config Files
COPY .bashrc .bashrc
COPY spark-env.sh /home/hadoop/spark/conf/spark-env.sh
COPY slaves /home/hadoop/spark/conf/slaves
COPY yarn-site.xml hadoop/etc/hadoop/yarn-site.xml

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
EXPOSE 10000
EXPOSE 4040
EXPOSE 8042
EXPOSE 8030
EXPOSE 8031
EXPOSE 8032
EXPOSE 8888

# Start startup Script
ENTRYPOINT ["/startup.sh"]


