import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "twitter", table_name = "tweets", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("favorites", "bigint", "favorites", "bigint"), ("created_at", "timestamp", "created_at", "timestamp"), ("id", "bigint", "id", "bigint"), ("tweets", "string", "tweets", "string"), ("retweets", "bigint", "retweets", "bigint"), ("inserted_at", "timestamp", "inserted_at", "timestamp"), ("user", "string", "user", "string")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["favorites", "created_at", "id", "tweets", "retweets", "inserted_at", "user"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "twitter", table_name = "ddb_tweets", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_struct", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "twitter", table_name = "ddb_tweets", transformation_ctx = "datasink5")
job.commit()