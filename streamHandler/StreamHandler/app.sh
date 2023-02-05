
## to build jar file
    ## sbt package
#/opt/spark/bin/spark-submit --master local --deploy-mode client --num-executors 2 --driver-memory 4g --executor-memory 2g --executor-cores 2  app.py 


# deploy jar file 
/opt/spark/bin/spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:3.1.2,org.apache.kafka:kafka-clients:3.2.1"  target/scala-2.11/spark-daria_2.11-1.0.jar

