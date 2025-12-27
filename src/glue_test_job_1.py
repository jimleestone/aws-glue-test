import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, floor, months_between, current_date
from awsglue.transforms import *

# Configuration
OUTPUT_S3_PATH = "s3://glue-test-target-bucket-1-379867926836-ap-northeast-1/data"
catalog_options = {
    "database": "glue_test_source_db_1",
    "table": "glue_test_source_table_1",
}


class GlueTestJob:
    def __init__(self):
        # parse job parameters
        params = []
        if "--JOB_NAME" in sys.argv:
            params.append("JOB_NAME")
        args = getResolvedOptions(sys.argv, params)

        # initialize Glue context and job
        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)

        # initialize job with name
        if "JOB_NAME" in args:
            jobname = args["JOB_NAME"]
        else:
            jobname = "glue_test_job_1"
        self.job.init(jobname, args)

    def run(self):
        # read data from catalog
        source_dyf = read_catalog(self.context)

        # convert DynamicFrame to Spark DataFrame
        df = source_dyf.toDF()

        # transform data
        df = df.withColumn(
            "age",
            floor(months_between(current_date(), col("birthday")) / 12).cast("integer"),
        )

        # convert pandas DataFrame to Spark DataFrame
        dyf = DynamicFrame.fromDF(
            df,
            self.context,
            "transformed_data_frame",
        )

        # repartition to single file
        single_dyf = dyf.repartition(1)
        dyf.printSchema()

        # apply mapping
        apply_mapping = ApplyMapping.apply(
            frame=single_dyf,
            mappings=[
                ("username", "string", "username", "string"),
                ("full_name", "string", "full_name", "string"),
                ("age", "int", "age", "int"),
                ("birthday", "date", "birthday", "date"),
                ("bio", "string", "bio", "string"),
            ],
            transformation_ctx="apply_mapping",
        )
        apply_mapping.printSchema()

        # write data to database
        write_s3(self.context, apply_mapping, OUTPUT_S3_PATH)

        self.job.commit()


# Read data from Glue Data Catalog
def read_catalog(glue_context: GlueContext) -> DynamicFrame:
    dynamicframe = glue_context.create_dynamic_frame.from_catalog(
        database=catalog_options["database"],
        table_name=catalog_options["table"],
        transformation_ctx="source_data",
    )

    return dynamicframe


# Write data to S3 in CSV format
def write_s3(glue_context: GlueContext, source, output_path: str):
    glue_context.write_from_options(
        frame_or_dfc=source,
        connection_type="s3",
        connection_options={"path": output_path},
        format="csv",
        format_options={
            "separator": ",",
            "quoteChar": '"',
            "escapeChar": "\\",
            "header": "True",
        },
    )


# Run the Glue job
if __name__ == "__main__":
    GlueTestJob().run()
