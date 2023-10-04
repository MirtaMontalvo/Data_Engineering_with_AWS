import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1695945539582 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://montalv1/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1695945539582",
)

# Script generated for node JoinPrivacy
JoinPrivacy_node1695945525573 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1695945539582,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinPrivacy_node1695945525573",
)

# Script generated for node DropCustomerFields
DropCustomerFields_node1695945716059 = DropFields.apply(
    frame=JoinPrivacy_node1695945525573,
    paths=[
        "phone",
        "email",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "registrationdate",
        "serialnumber",
        "birthday",
        "customername",
    ],
    transformation_ctx="DropCustomerFields_node1695945716059",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1695945844196 = glueContext.getSink(
    path="s3://montalv1/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1695945844196",
)
accelerometer_trusted_node1695945844196.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1695945844196.setFormat("json")
accelerometer_trusted_node1695945844196.writeFrame(DropCustomerFields_node1695945716059)
job.commit()
