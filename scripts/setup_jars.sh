# Create setup script
#!/bin/bash
# Setup script for JARs to be used in dockerfiles
ENV SPARK_VERSION=3.5.6
ENV SPARK_MAJOR_VERSION=3.5

# Create jars directory
mkdir -p spark/jars

# Download all JARs (force download)
echo "Downloading iceberg-spark-runtime JAR..."
wget -O spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar

echo "Downloading iceberg-aws-bundle JAR..."
wget -O spark/jars/iceberg-aws-bundle-1.9.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar

echo "Downloading spark-connect client JAR..."
wget -O spark/jars/spark-connect_2.12-3.5.6.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.6/spark-connect_2.12-3.5.6.jar

echo "JARs setup complete!"
echo "You can now run: uv run sqlmesh plan --gateway spark"
