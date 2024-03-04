FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install apache-airflow==2.7.1 \
                apache-airflow-providers-apache-spark \
                pyspark \
                plotly \
                pandas==2.0 \
                sqlalchemy==1.4.50 \
                reportlab \
                python-dotenv \