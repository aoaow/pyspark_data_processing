from pyspark.sql import SparkSession
from pyspark.sql.types import (
     StructType, 
     StructField, 
     StringType, 
     DateType,
     FloatType
)
import pyspark.sql.functions as F

LOG_FILE = "pyspark_output.md"

def log_output(operation, output, query=None):
    """Adds to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"## Operation: {operation}\n\n")
        if query: 
            file.write(f"### Query:\n\n```sql\n{query}\n```\n\n")
        file.write("### Output:\n\n")
        file.write(output)
        file.write("\n\n---\n\n")

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "Spark session stopped."

def load_data(spark, data_path="data/serious-injury-outcome-indicators-2000-2022.csv", view_name="injuryOutcome"):
    """Load data with a predefined schema."""
    schema = StructType([
        StructField("Series_reference", StringType(), True),
        StructField("Period", DateType(), True),
        StructField("Type", StringType(), True),
        StructField("Data_value", FloatType(), True),
        StructField("Lower_CI", FloatType(), True),
        StructField("Upper_CI", FloatType(), True),
        StructField("Units", StringType(), True),
        StructField("Indicator", StringType(), True),
        StructField("Cause", StringType(), True),
        StructField("Validation", StringType(), True),
        StructField("Population", StringType(), True),
        StructField("Age", StringType(), True),
        StructField("Severity", StringType(), True)
    ])

    df = spark.read.csv(data_path, header=True, schema=schema)

    # Log initial data
    log_output("Load Data", df.limit(10).toPandas().to_markdown())

    return df

def query_data(spark, df):
    """Perform a Spark SQL query."""
    query = """
    SELECT Severity, COUNT(*) AS total_cases
    FROM injuryOutcome
    GROUP BY Severity
    ORDER BY total_cases DESC
    """

    # Create a temporary view for SQL queries
    df = df.createOrReplaceTempView("injuryOutcome")

    result_df = spark.sql(query)

    # Log query results
    log_output("Spark SQL Query", result_df.toPandas().to_markdown(), query)

    result_df.show()

def transform_data(df):
    """Perform data transformations."""
    df_transformed = df.withColumn("year", F.year("Period")) \
        .withColumn("before_2010", F.when(F.col("year") < 2010, 1).otherwise(0))

    # Log transformed data
    log_output("Data Transformation", df_transformed.limit(10).toPandas().to_markdown())

    df_transformed.show()
    return df_transformed

if __name__ == "__main__":
    # Start Spark session
    spark = start_spark("InjuryOutcomeAnalysis")

    # Load data
    df = load_data(spark)

    # Perform SQL query
    query_data(spark, df)

    # Perform data transformation
    df_transformed = transform_data(df)

    # End Spark session
    end_spark(spark)
