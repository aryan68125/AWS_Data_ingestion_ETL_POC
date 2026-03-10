import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

from functools import reduce
from pyspark.sql import functions as F

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_key'
])

source_bucket = args['source_bucket']
source_key = args['source_key']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """Rules = [
    ColumnCount > 0,

    IsComplete "rating",
    IsComplete "rating_count",
    IsComplete "discounted_price",
    IsComplete "actual_price",

    ColumnValues "rating" >= 0,
    ColumnValues "rating_count" >= 0,
    ColumnValues "discounted_price" >= 0,
    ColumnValues "actual_price" >= 0,
    ColumnValues "discount_percentage" >= 0,
    
    IsUnique "product_id"
]"""

# -------- DELIMITER CORRUPTION CHECK -------- #
import csv
import io

raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()

if len(raw_lines) < 2:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

def count_csv_columns(line: str) -> int:
    """Count columns correctly, respecting quoted fields containing commas."""
    try:
        return len(next(csv.reader(io.StringIO(line))))
    except StopIteration:
        return 0

header_line = raw_lines[0][0]
expected_col_count = count_csv_columns(header_line)

bad_row_indices = []
for i, row in enumerate(raw_lines[1:], start=2):
    line = row[0]
    actual_col_count = count_csv_columns(line)
    if actual_col_count != expected_col_count:
        bad_row_indices.append((i, actual_col_count, line[:80]))

if bad_row_indices:
    details = "\n".join(
        [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
         for idx, actual, preview in bad_row_indices]
    )
    raise Exception(
        f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
        f"(likely semicolons used as delimiters instead of commas):\n{details}"
    )

print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
# -------- DELIMITER CORRUPTION CHECK -------- #

# Script generated for node Raw data source S3
RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
    # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
    transformation_ctx="RawdatasourceS3_node1772431168408")
# for debugging only
print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

df = RawdatasourceS3_node1772431168408.toDF()

print("Glue Visual ETL | DEBUG bucket:", source_bucket)
print("Glue Visual ETL | DEBUG key:", source_key)
print("Glue Visual ETL | DEBUG columns:", df.columns)
print("Glue Visual ETL | DEBUG row count:", df.count())
print("Glue Visual ETL | DEBUG schema:", df.schema)
print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())

# -------- CORRUPTION CHECK (SAFE VERSION) -------- #

# If Spark inferred no columns → corrupted file
if len(df.columns) == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

expected_cols = len(df.columns)

# If dataframe empty → corrupted file
if df.limit(1).count() == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

# Row-level corruption detection
null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

bad_rows = df.filter(
    reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
)

if bad_rows.limit(1).count() > 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")
# -------- CORRUPTION CHECK (SAFE VERSION) -------- #

# Script generated for node SQL Query
SqlQuery61 = '''
SELECT
    product_id,
    product_name,
    category,
    about_product,
    user_id,
    user_name,
    review_id,
    review_title,
    review_content,
    img_link,
    product_link,

    -- discounted_price: ₹149 → 149.0
    CAST(
        REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS discounted_price,

    -- actual_price: ₹1,000 → 1000.0
    CAST(
        REGEXP_REPLACE(actual_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS actual_price,

    -- discount_percentage: 85% → 85.0
    CAST(
        REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
        AS DOUBLE
    ) AS discount_percentage,

    -- rating: 3.9 → 3.9
    CAST(
        REGEXP_REPLACE(rating, '[^0-9.]', '')
        AS DOUBLE
    ) AS rating,

    -- rating_count: 24,871 → 24871
    CAST(
        REGEXP_REPLACE(rating_count, '[^0-9]', '')
        AS INT
    ) AS rating_count

FROM myDataSource;
'''
SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

# Script generated for node Drop Null Fields
DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

# Script generated for node Silver layer data sink S3
EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
job.commit()