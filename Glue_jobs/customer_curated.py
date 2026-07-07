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
customer_df=spark.read.parquet(
    "s3://stedi-samyugtha-01/customer_trusted/"
)
accelerometer_df=glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted"
).toDF()
joined_df=customer_df.join(
    accelerometer_df,
    customer_df["email"]==accelerometer_df["user"],
    "inner"
)
curated_df=joined_df.select(
    customer_df["customername"],
    customer_df["email"],
    customer_df["phone"],
    customer_df["birthday"],
    customer_df["serialnumber"],
    customer_df["registrationdate"],
    customer_df["lastupdatedate"],
    customer_df["sharewithresearchasofdate"],
    customer_df["sharewithpublicasofdate"],
    customer_df["sharewithfriendsasofdate"]
).dropDuplicates()

curated_dynamic = DynamicFrame.fromDF(
    curated_df,
    glueContext,
    "curated_dynamic"
)

sink = glueContext.getSink(
    path="s3://stedi-samyugtha-01/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_db",
    catalogTableName="customer_curated"
)

sink.setFormat("glueparquet")

sink.writeFrame(curated_dynamic)

job.commit()
