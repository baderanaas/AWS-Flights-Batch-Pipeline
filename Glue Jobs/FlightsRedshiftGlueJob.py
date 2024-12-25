import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'])

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="flights_db",
    table_name="all_data"
)

redshift_properties = {
    "url": "jdbc:redshift://redshift-link:port/dbname",
    "dbtable": "schema.table",
    "user": "admin",
    "password": "password",
    "redshiftTmpDir": "s3://flights-data-storage/redshift/temp/"
}

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options=redshift_properties,
    transformation_ctx="write_redshift"
)

job.commit()