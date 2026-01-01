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

# Script generated for node step trainer landing
steptrainerlanding_node1767260372832 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="steptariner_landing_table", transformation_ctx="steptrainerlanding_node1767260372832")

# Script generated for node customer curated
customercurated_node1767260374062 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customercurated_node1767260374062")

# Script generated for node SQL Query
SqlQuery2480 = '''
select * from t1
join t2 on t1.serialNumber=t2.serialNumber
'''
SQLQuery_node1767260755350 = sparkSqlQuery(glueContext, query = SqlQuery2480, mapping = {"t1":customercurated_node1767260374062, "t2":steptrainerlanding_node1767260372832}, transformation_ctx = "SQLQuery_node1767260755350")

# Script generated for node Drop Fields
DropFields_node1767260385292 = DropFields.apply(frame=SQLQuery_node1767260755350, paths=["birthDay", "shareWithPublicAsOfDate", "shareWithResearchAsOfDate", "registrationDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone"], transformation_ctx="DropFields_node1767260385292")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1767260385292, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767260222208", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767260392764 = glueContext.getSink(path="s3://sparkgluepythonproject/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767260392764")
AmazonS3_node1767260392764.setCatalogInfo(catalogDatabase="stedi",catalogTableName="steptrainer trusted")
AmazonS3_node1767260392764.setFormat("glueparquet", compression="snappy")
AmazonS3_node1767260392764.writeFrame(DropFields_node1767260385292)
job.commit()