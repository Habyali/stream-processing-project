FROM flink:1.18-java11

# Install Python and PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Download required JARs for Flink connectors
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/redis/clients/jedis/5.1.0/jedis-5.1.0.jar

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY process_engagement.py .

# Set environment for PyFlink
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin

CMD ["python", "-u", "process_engagement.py"]