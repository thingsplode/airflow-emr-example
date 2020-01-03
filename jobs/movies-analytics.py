import argparse
from pyspark.sql import SparkSession


def main(
        input_path,
        output_path):
    """Create Spark Session and execute `Job.run`."""
    spark = SparkSession.builder.appName('Movie Analytics').getOrCreate()

    movie_df = spark.read \
        .format("csv") \
        .option("mode", "DROPMALFORMED") \
        .option("header", "true") \
        .load(input_path + "/movies.csv")

    raitings_df = spark.read.format("csv") \
        .option("mode", "DROPMALFORMED") \
        .option("header", "true") \
        .load(input_path + "/ratings.csv")

    joined_df = raitings_df.join(movie_df, movie_df.movieId == raitings_df.movieId)

    joined_df.registerTempTable("movies_ratings")
    result_df = spark.sql(
        "select title, "
        "sum(rating)/count(*) as weight_avg, "
        "count(*) as num_votes  "
        "from movies_ratings group by title order by num_votes desc")

    result_df.repartition(3).write.format("csv").mode("overwrite").options(header="true").save(
        path=output_path + "/ratings_result")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-i', '--input_path',
        help='Input file path, accepts multiple eg. --input=path1 path2',
        required=True)

    parser.add_argument(
        '-o', '--output_path',
        help='Output path',
        required=True)

    args = parser.parse_args()

    main(
        input_path=args.input_path,
        output_path=args.output_path)
