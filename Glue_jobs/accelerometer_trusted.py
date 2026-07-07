import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

accelerometer_df=glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer"
).toDF()

customer_df=glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted"
).toDF()

accelerometer_df=accelerometer_df.withColumn("user",col("user").cast("string"))
customer_df=customer_df.withColumn("email",col("email").cast("string"))
joined_df=accelerometer_df.join(
    customer_df,
    accelerometer_df["user"]==customer_df["email"],
    "inner"
)

final_df=joined_df.select(
    accelerometer_df["user"],
    accelerometer_df["timestamp"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"]
)

final_dynamic = DynamicFrame.fromDF(
    final_df,
    glueContext,
    "final_dynamic"
)

sink = glueContext.getSink(
    path="s3://stedi-samyugtha-01/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_db",
    catalogTableName="accelerometer_trusted"
)

sink.setFormat("glueparquet")

sink.writeFrame(final_dynamic)



job.commit()
