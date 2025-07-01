# Create setup script
#!/bin/bash
# Setup script for SQLMesh JARs

echo "Setting up SQLMesh JARs..."

# Create jars directory
mkdir -p transform/jars

# Download JARs if they don't exist
if [ ! -f "transform/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar" ]; then
    echo "Downloading iceberg-spark-runtime JAR..."
    wget -O transform/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar
fi

if [ ! -f "transform/jars/iceberg-aws-bundle-1.9.1.jar" ]; then
    echo "Downloading iceberg-aws-bundle JAR..."
    wget -O transform/jars/iceberg-aws-bundle-1.9.1.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar
fi

echo "JARs setup complete!"
echo "You can now run: uv run sqlmesh plan --gateway spark"
