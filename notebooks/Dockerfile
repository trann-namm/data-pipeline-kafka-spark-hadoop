FROM jupyter/scipy-notebook

# Install Java and PySpark
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install PySpark
RUN pip install pyspark
# Create a new user (e.g., "trannam") and set home directory
RUN useradd -m -s /bin/bash trannam \
    && echo "trannam ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Change ownership of the working directory
RUN chown -R trannam:trannam /home/jovyan
USER trannam
WORKDIR /home/trannam/