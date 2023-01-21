FROM spark-base

# -- Runtime
ARG spark_master_web_ui=8008

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

COPY ./spark-defaults.conf conf/spark-defaults.conf

COPY ./spark-env.conf conf/spark-env.conf

#Create the workspace/events shared dir and start Spark Master

CMD bash -c "mkdir -p /opt/workspace/events/twitter && mkdir -p /opt/workspace/tmp/driver/twitter && mkdir -p /opt/workspace/tmp/executor/twitter && bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out"

