import argparse
from pyspark.sql import SparkSession


def main(
        input_path,
        output_path):
    spark = SparkSession.builder.appName('Movie Analytics').getOrCreate()

    movie_df = spark.read \
        .format("csv") \
        .option("mode", "DROPMALFORMED") \
        .option("header", "true") \
        .load(f"{input_path}/movies.csv")

    ratings_df = spark.read.format("csv") \
        .option("mode", "DROPMALFORMED") \
        .option("header", "true") \
        .load(f"{input_path}/ratings.csv")

    joined_df = ratings_df.join(movie_df, movie_df.movieId == ratings_df.movieId)

    joined_df.registerTempTable("movies_ratings")
    result_df = spark.sql(
        "select title, "
        "sum(rating)/count(*) as weight_avg, "
        "count(*) as num_votes  "
        "from movies_ratings group by title order by num_votes desc")

    result_df.\
        coalesce(3).\
        write.\
        format("csv").\
        mode("overwrite").\
        options(header="true").\
        save(path=f'{output_path}/ratings_result')


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
