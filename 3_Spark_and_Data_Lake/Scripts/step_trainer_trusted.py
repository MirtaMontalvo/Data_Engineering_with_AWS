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

# Script generated for node step_trainer_landing
step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://montalv1/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1",
)

# Script generated for node customer_curated
customer_curated_node1696430734043 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1696430734043",
)

# Script generated for node Join_by_SerialNumber
Join_by_SerialNumber_node1696430866197 = Join.apply(
    frame1=customer_curated_node1696430734043,
    frame2=step_trainer_landing_node1,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_by_SerialNumber_node1696430866197",
)

# Script generated for node Drop_Customer_Fields
Drop_Customer_Fields_node1696430970084 = DropFields.apply(
    frame=Join_by_SerialNumber_node1696430866197,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="Drop_Customer_Fields_node1696430970084",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1696431090743 = glueContext.getSink(
    path="s3://montalv1/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1696431090743",
)
step_trainer_trusted_node1696431090743.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1696431090743.setFormat("json")
step_trainer_trusted_node1696431090743.writeFrame(
    Drop_Customer_Fields_node1696430970084
)
job.commit()
