import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer trusted
Customertrusted_node1767275020405 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer trusted", transformation_ctx="Customertrusted_node1767275020405")

# Script generated for node accelerometer landing
accelerometerlanding_node1767275021290 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing_table", transformation_ctx="accelerometerlanding_node1767275021290")

# Script generated for node Join
Join_node1767275154471 = Join.apply(frame1=accelerometerlanding_node1767275021290, frame2=Customertrusted_node1767275020405, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1767275154471")

# Script generated for node SQL Query
SqlQuery2404 = '''
select user,timestamp,x,y,z from myDataSource

'''
SQLQuery_node1767275549222 = sparkSqlQuery(glueContext, query = SqlQuery2404, mapping = {"myDataSource":Join_node1767275154471}, transformation_ctx = "SQLQuery_node1767275549222")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767275549222, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767273753672", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767275257970 = glueContext.getSink(path="s3://sparkgluepythonproject/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767275257970")
AmazonS3_node1767275257970.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer trusted")
AmazonS3_node1767275257970.setFormat("json")
AmazonS3_node1767275257970.writeFrame(SQLQuery_node1767275549222)
job.commit()