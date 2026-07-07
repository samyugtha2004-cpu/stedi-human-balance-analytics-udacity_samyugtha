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

step_df=glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer"
).toDF()

customer_df=spark.read.parquet(
    "s3://stedi-samyugtha-01/customer_curated/"
)
step_df=step_df.withColumn(
    "serialnumber",
    col("serialnumber").cast("string")
)
customer_serials=customer_df.select("serialnumber").dropDuplicates()
trusted_df=step_df.join(
    customer_serials,
    "serialnumber",
    "inner"
)

trusted_dynamic = DynamicFrame.fromDF(
    trusted_df,
    glueContext,
    "trusted_dynamic"
)

sink = glueContext.getSink(
    path="s3://stedi-samyugtha-01/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_db",
    catalogTableName="step_trainer_trusted"
)

sink.setFormat("glueparquet")

sink.writeFrame(trusted_dynamic)

job.commit()
