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

# Script generated for node customer curated
customercurated_node1767279604916 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer curated", transformation_ctx="customercurated_node1767279604916")

# Script generated for node step trainer landing
steptrainerlanding_node1767279605659 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="steptariner_landing_table", transformation_ctx="steptrainerlanding_node1767279605659")

# Script generated for node SQL Query
SqlQuery2154 = '''
select * from t1
join t2 on t1.serialnumber=t2.serialnumber

'''
SQLQuery_node1767279616912 = sparkSqlQuery(glueContext, query = SqlQuery2154, mapping = {"t1":steptrainerlanding_node1767279605659, "t2":customercurated_node1767279604916}, transformation_ctx = "SQLQuery_node1767279616912")

# Script generated for node Drop Fields
DropFields_node1767279801346 = DropFields.apply(frame=SQLQuery_node1767279616912, paths=["birthDay", "shareWithPublicAsOfDate", "shareWithResearchAsOfDate", "registrationDate", "shareWithFriendsAsOfDate", "customerName", "email", "phone", "lastUpdateDate"], transformation_ctx="DropFields_node1767279801346")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1767279801346, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767279598498", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767279806183 = glueContext.getSink(path="s3://sparkgluepythonproject/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767279806183")
AmazonS3_node1767279806183.setCatalogInfo(catalogDatabase="stedi",catalogTableName="steptrainer trusted")
AmazonS3_node1767279806183.setFormat("json")
AmazonS3_node1767279806183.writeFrame(DropFields_node1767279801346)
job.commit()