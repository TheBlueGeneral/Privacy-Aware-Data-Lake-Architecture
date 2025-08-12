# Use an official Spark image as a base
FROM bitnami/spark:3.3.4

# The JARs need to be in Spark's classpath. The /opt/bitnami/spark/jars directory is a standard location.
# Copy the JARs we downloaded into the container's jars directory.
COPY ./jars/* /opt/bitnami/spark/jars/