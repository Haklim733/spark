# docker exec spark-master spark-submit /home/app/src/test_job.py
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name "CreateTables" \
  --conf "spark.jars=/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar" \
  --conf "spark.submit.pyFiles=" \
  --conf "spark.submit.deployMode=client" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=/opt/bitnami/spark/logs/spark-events" \
  /home/app/src/create_table.py

# docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --name "check tables" \
#   /home/app/src/check_tables.py