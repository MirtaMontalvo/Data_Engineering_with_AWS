import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://montalv1/customer/landing/"], "recurse": True},
    transformation_ctx="customer_landing_node1",
)

# Script generated for node privacyFilter
privacyFilter_node1695911776744 = Filter.apply(
    frame=customer_landing_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="privacyFilter_node1695911776744",
)

# Script generated for node customer_trusted
customer_trusted_node1696007010042 = glueContext.getSink(
    path="s3://montalv1/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1696007010042",
)
customer_trusted_node1696007010042.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
customer_trusted_node1696007010042.setFormat("json")
customer_trusted_node1696007010042.writeFrame(privacyFilter_node1695911776744)
job.commit()