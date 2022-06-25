"""Sparkify Data Lake Transformations."""
import configparser
import datetime
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    TimestampType,
    StructType,
    StringType,
    StructField,
)
from pyspark.sql.functions import (
    udf,
    year,
    month,
    quarter,
    weekofyear,
    split,
    col,
    upper,
    sum,
    avg,
    mean,
    expr,
)

config = configparser.ConfigParser()
config.read("dl.cfg")
os.environ["AWS_ACCESS_KEY_ID"] = config["default"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["default"][
    "AWS_SECRET_ACCESS_KEY"
]


@udf(TimestampType())
def sas_to_date(days):
    """Change date format for columns start with 1960 year as a base number."""
    if not days:
        return None
    return datetime.datetime(1960, 1, 1) + datetime.timedelta(days=int((days)))


def create_spark_session():
    """Create Spark Session."""
    spark = (
        SparkSession.builder.config(
            "spark.jars.repositories", "https://repos.spark-packages.org/"
        )
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def get_data_from_description_file(spark, pattern):
    """Return spark dataframe based on pattern from the static file."""
    result = {}
    with open("I94_SAS_Labels_Descriptions.SAS") as labels_file:
        file_data = labels_file.read()
        data = re.search(rf"{pattern}", file_data).group(0)
        result.update(
            {
                item.replace("'", "")
                .replace("\t", "")
                .split("=")[0]
                .strip(" "): item.replace("'", "")
                .replace("\t", "")
                .split("=")[1]
                .strip(" ")
                for item in data.split("\n")
                if "=" in item
            }
        )

    return spark.createDataFrame(
        result.items(),
        schema=StructType(
            fields=[
                StructField("code", StringType()),
                StructField("name", StringType()),
            ]
        ),
    )


def clean_airports_data(air_df):
    """Get rows with value for iata_code column."""
    air_df = air_df.filter(~col("iata_code").isNull())
    air_df = air_df.dropDuplicates(["iata_code"])
    return air_df


def create_airports_table(spark):
    """Create aiports table based on the code value of static file."""
    # get the US ports from static text file
    df_us_ports = get_data_from_description_file(spark, "I94PORT[^;]+")
    df_us_ports = df_us_ports.withColumn(
        "city", split(col("name"), ",").getItem(0)
    ).withColumn("state", split(col("name"), ",").getItem(1))
    df_us_ports = df_us_ports.select(["code", "city", "state"])

    df_airport = spark.read.csv(config["path"]["AIRPORTS_PATH"], header=True)
    df_airport = clean_airports_data(df_airport)

    aiports = df_us_ports.alias("us_ports").join(
        df_airport.alias("airports"),
        expr("us_ports.code = airports.iata_code"),
        "left",
    )
    aiports = aiports.withColumnRenamed("code", "airport_id")
    aiports = aiports.dropDuplicates(["airport_id"])
    aiports.write.mode("overwrite").parquet(
        config["ouput_path"]["AIRPORTS_OUTPATH"]
    )


def clean_states_data(df_us_state):
    """Clean States dataset and groub by data with state_code column."""
    for column in df_us_state.columns:
        df_us_state = df_us_state.withColumnRenamed(column, column.lower())
    df_us_state = df_us_state.groupBy("state code").agg(
        sum("male population").alias("male_population"),
        sum("female population").alias("female_population"),
        sum("total population").alias("total_population"),
        sum("number of veterans").alias("veteran_num"),
        avg("average household size").alias("household_avg"),
        mean("median age").alias("median_age"),
    )
    df_us_state = df_us_state.withColumnRenamed("state code", "state_id")

    return df_us_state


def create_states_table(spark):
    """Clean states dataset and create states table."""
    # Load US states dataset
    df_us_state = spark.read.csv(
        config["path"]["STATES_PATH"], sep=";", header=True
    )
    df_us_state = clean_states_data(df_us_state)
    df_us_state.write.mode("overwrite").parquet(
        config["ouput_path"]["STATES_OUTPATH"]
    )


def clean_countries_data(df_countries):
    """Clean country dataset and map columns."""
    for column in df_countries.columns:
        df_countries = df_countries.withColumnRenamed(column, column.lower())

    df_countries = (
        df_countries.withColumnRenamed("country (or dependency)", "name")
        .withColumnRenamed("population (2020)", "population")
        .withColumnRenamed("yearly change", "yearly_change")
        .withColumnRenamed("net change", "net_change")
        .withColumnRenamed("Density (P/Km²)", "density")
        .withColumnRenamed("Land Area (Km²)", "land_area")
        .withColumnRenamed("Migrants (net)", "migrants_net")
        .withColumnRenamed("Fert. Rate", "fert_rate")
        .withColumnRenamed("Med. Age", "med_age")
        .withColumnRenamed("urban pop %", "urban_pop%")
        .withColumnRenamed("world share", "world_share")
    )
    df_countries = df_countries.withColumn("name", upper(col("name")))

    return df_countries


def create_countries_table(spark):
    """Create country dataset."""
    # Get country code and name from static file
    df_country = get_data_from_description_file(spark, "I94CIT & I94RES[^;]+")
    df_country = df_country.withColumnRenamed("name", "country")

    # Load countries from countries dataset
    df_countries = spark.read.csv(
        config["path"]["COUNTRIES_PATH"], header=True
    )
    df_countries = clean_countries_data(df_countries)
    countries = df_country.alias("countries").join(
        df_countries.alias("world"),
        expr("countries.country like world.name"),
        "left",
    )
    countries = countries.withColumnRenamed("code", "country_id")

    countries.write.mode("overwrite").parquet(
        config["ouput_path"]["COUNTRIES_OUTPATH"]
    )


def create_admissions_table(df):
    """Create admissions table from immigration dataset."""
    admission_columns = [
        "admnum",
        "visatype",
        "insnum",
        "dtaddto",
        "gender",
        "biryear",
        "i94cit",
        "i94res",
        "i94mode",
        "airline",
        "fltno",
        "occup",
    ]
    adm_tble = df.select(admission_columns).dropDuplicates(["admnum"])
    adm_tble = (
        adm_tble.withColumnRenamed("admnum", "admission_id")
        .withColumnRenamed("occup", "occupation")
        .withColumnRenamed("fltno", "flight_number")
        .withColumnRenamed("i94cit", "city")
        .withColumnRenamed("i94res", "resident_city")
        .withColumnRenamed("i94mode", "mode")
    )
    adm_tble = adm_tble.dropDuplicates(["admission_id"])

    adm_tble.write.mode("overwrite").parquet(
        config["ouput_path"]["ADMISSIONS_OUTPATH"]
    )


def create_time_table(df):
    """Create time table from immigration dataset."""
    df = df.select("arrdate").distinct()
    df = df.withColumn("time_id", col("arrdate").cast("long"))
    df = df.withColumn("year", year(col("arrdate")))
    df = df.withColumn("month", month(col("arrdate")))
    df = df.withColumn("quarter", quarter(col("arrdate")))
    df = df.withColumn("year_week", weekofyear(col("arrdate")))

    df = df.withColumnRenamed("arrdate", "date")

    df.write.mode("overwrite").parquet(config["ouput_path"]["TIME_OUTPATH"])


def create_fact_data(df):
    """Create time table from immigration dataset."""
    df = df.select(
        col("cicid").alias("fact_id"),
        col("admnum").alias("admission_id"),
        col("i94addr").alias("state_id"),
        col("i94port").alias("airports_id"),
        col("i94yr").alias("year"),
        col("i94mon").alias("month"),
        col("arrdate").alias("arrival_date"),
        col("depdate").alias("departure_date"),
    )
    df = df.withColumn("time_id", col("arrival_date").cast("long"))
    df = df.withColumn(
        "duration",
        (
            (
                col("departure_date").cast("long")
                - col("arrival_date").cast("long")
            )
            / 60.0
            / 60.0
            / 24.0
        ).cast("integer"),
    )

    df = df.drop("arrival_date")
    df.dropDuplicates(["fact_id"])

    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        config["ouput_path"]["FACT_DATA_OUTPATH"]
    )


def process_data(spark):
    """Process data for creating fact_data & dimensional tables."""
    df_spark = spark.read.parquet(config["path"]["IMMIGRANTS_PATH"])
    df_spark = clean_immigrants_data(df_spark)

    create_airports_table(spark)

    create_states_table(spark)

    create_countries_table(spark)

    create_admissions_table(df_spark)

    create_time_table(df_spark)

    create_fact_data(df_spark)


def clean_immigrants_data(df_spark):
    """Clean immigrants dataset."""
    # Change type of i94yr','i94mon','i94cit','i94res',
    # 'i94mode','biryear','i94visa' columns to Integer."""
    integer_cols = [
        "i94yr",
        "i94mon",
        "i94cit",
        "i94res",
        "i94mode",
        "biryear",
        "i94visa",
    ]
    for item in integer_cols:
        df_spark = df_spark.withColumn(item, col(item).cast(IntegerType()))

    # Change type of 'admnum' and 'cicid' columns to Long
    df_spark = df_spark.withColumn("admnum", col("admnum").cast(LongType()))
    df_spark = df_spark.withColumn("cicid", col("cicid").cast(LongType()))
    # change date format of 'arrdate' and 'depdate' columns
    df_spark = df_spark.withColumn("arrdate", sas_to_date(col("arrdate")))
    df_spark = df_spark.withColumn("depdate", sas_to_date(col("depdate")))

    # we only want rows which have data for departure_date column
    df_spark = df_spark.where(~col("depdate").isNull())

    return df_spark


def main():
    """Trigger all transformations."""
    spark = create_spark_session()

    process_data(spark)


if __name__ == "__main__":
    main()
