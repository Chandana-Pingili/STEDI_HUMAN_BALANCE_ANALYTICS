import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1767276145898 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer trusted", transformation_ctx="Accelerometertrusted_node1767276145898")

# Script generated for node customer trusted
customertrusted_node1767276146750 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer trusted", transformation_ctx="customertrusted_node1767276146750")

# Script generated for node Join
Join_node1767276265303 = Join.apply(frame1=customertrusted_node1767276146750, frame2=Accelerometertrusted_node1767276145898, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1767276265303")

# Script generated for node Drop Duplicates
DropDuplicates_node1767276420903 =  DynamicFrame.fromDF(Join_node1767276265303.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1767276420903")

# Script generated for node Drop Fields
DropFields_node1767276484400 = DropFields.apply(frame=DropDuplicates_node1767276420903, paths=["z", "y", "user", "x", "timestamp"], transformation_ctx="DropFields_node1767276484400")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1767276484400, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767276137182", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767276598208 = glueContext.getSink(path="s3://sparkgluepythonproject/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767276598208")
AmazonS3_node1767276598208.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer curated")
AmazonS3_node1767276598208.setFormat("json")
AmazonS3_node1767276598208.writeFrame(DropFields_node1767276484400)
job.commit()