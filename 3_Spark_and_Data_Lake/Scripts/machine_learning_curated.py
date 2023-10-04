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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1696381203690 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1696381203690",
)

# Script generated for node Join_by_timestamp
Join_by_timestamp_node1696381307913 = Join.apply(
    frame1=step_trainer_trusted_node1,
    frame2=accelerometer_trusted_node1696381203690,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_by_timestamp_node1696381307913",
)

# Script generated for node Drop_PII
Drop_PII_node1696381769375 = DropFields.apply(
    frame=Join_by_timestamp_node1696381307913,
    paths=["user"],
    transformation_ctx="Drop_PII_node1696381769375",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1696381838513 = glueContext.getSink(
    path="s3://montalv1/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1696381838513",
)
machine_learning_curated_node1696381838513.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1696381838513.setFormat("json")
machine_learning_curated_node1696381838513.writeFrame(Drop_PII_node1696381769375)
job.commit()
