import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_frame=glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer"
)
customer_df=customer_frame.toDF()
trusted_df=customer_df.filter(
    customer_df["sharewithresearchasofdate"]>0
)
trusted_dynamic=DynamicFrame.fromDF(
    trusted_df,
    glueContext,
    "trusted_dynamic"
)

sink = glueContext.getSink(
    path="s3://stedi-samyugtha-01/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_db",
    catalogTableName="customer_trusted"
)

sink.setFormat("glueparquet")

sink.writeFrame(trusted_dynamic)


job.commit()
