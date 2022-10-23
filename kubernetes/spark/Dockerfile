
FROM gcr.io/spark-operator/spark:v3.1.1

# using root
USER root:root

# create directory for applications
RUN mkdir -p /app 

#copy jars files
COPY ./jars/ /opt/spark/jars

#set main work directory
WORKDIR /app

#user
USER root