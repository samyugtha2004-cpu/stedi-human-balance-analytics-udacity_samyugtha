import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_df=spark.read.parquet(
    "s3://stedi-samyugtha-01/step_trainer_trusted/"
)

acc_df=spark.read.parquet(
    "s3://stedi-samyugtha-01/accelerometer_trusted/"
)

acc_df=acc_df.withColumnRenamed(
    "timestamp",
    "sensorreadingtime"
)

machine_learning_curated_df=step_df.join(
    acc_df,
    on="sensorreadingtime",
    how="inner"
)

machine_learning_curated_df=machine_learning_curated_df.select(
    step_df["serialnumber"],
    step_df["sensorreadingtime"],
    step_df["distancefromobject"],
    acc_df["x"],
    acc_df["y"],
    acc_df["z"]
)

machine_learning_dynamic = DynamicFrame.fromDF(
    machine_learning_curated_df,
    glueContext,
    "machine_learning_dynamic"
)

sink = glueContext.getSink(
    path="s3://stedi-samyugtha-01/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_db",
    catalogTableName="machine_learning_curated"
)

sink.setFormat("glueparquet")

sink.writeFrame(machine_learning_dynamic)

job.commit()
