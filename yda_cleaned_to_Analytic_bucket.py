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

# Script generated for node yda_cleaned_reference_data
yda_cleaned_reference_data_node1685966487651 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="de-project-yda-glue-database-cleansed-dev",
        table_name="cleansed_statistics_reference_data",
        transformation_ctx="yda_cleaned_reference_data_node1685966487651",
    )
)

# Script generated for node yda_cleaned_statistics
yda_cleaned_statistics_node1685966569993 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="de-project-yda-glue-database-cleansed-dev",
        table_name="statistics",
        transformation_ctx="yda_cleaned_statistics_node1685966569993",
    )
)

# Script generated for node Inner join on Id and Category_id
InnerjoinonIdandCategory_id_node1685966607680 = Join.apply(
    frame1=yda_cleaned_reference_data_node1685966487651,
    frame2=yda_cleaned_statistics_node1685966569993,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="InnerjoinonIdandCategory_id_node1685966607680",
)

# Script generated for node Analystical Target
AnalysticalTarget_node1685966861456 = glueContext.getSink(
    path="s3://de-project-yda-analyticbucket",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["category_id"],
    enableUpdateCatalog=True,
    transformation_ctx="AnalysticalTarget_node1685966861456",
)
AnalysticalTarget_node1685966861456.setCatalogInfo(
    catalogDatabase="db_yda_analytics", catalogTableName="FInal_Analytics"
)
AnalysticalTarget_node1685966861456.setFormat("glueparquet")
AnalysticalTarget_node1685966861456.writeFrame(
    InnerjoinonIdandCategory_id_node1685966607680
)
job.commit()
