# -*- coding: utf-8 -*-

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import (
    TimestampType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    FloatType)


class MovieDataProcessor(object):
    """Class that processes movie data set csv files.  See source of data here
    https://www.kaggle.com/rounakbanik/the-movies-dataset.

    Must be run on with local Spark or on a Spark cluster in EMR or something
    similar.

    Attributes:
        SPARK (pyspark.sql.session.SparkSession): Spark session for class
        COMMON_BASE_SCHEMA (pyspark.sql.types.ArrayType): Commonly used Pyspark
        schema with basic `id` and `name` keys.  Used to process JSON strings in
        multiple csv files
        CASTS_SCHEMA (pyspark.sql.types.ArrayType): Pyspark schema for extracting
        casts JSON string in credits.csv file
        CREDITS_SCHEMA (pyspark.sql.types.ArrayType): Pyspark schema for extracting
        credits JSON string in credits.csv file
        COLLECTIONS_SCHEMA (pyspark.sql.types.StructType): Pyspark schema for extracting
        belongs_to_collection JSON string in movies_metadata.csv file
        base_s3_destination (str): Base S3 path to place processed JSON files.
        Each table file will be separated into a different prefix under this location
    """
    SPARK = SparkSession.builder.appName("MovieProcessApp").getOrCreate()
    COMMON_BASE_SCHEMA = ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())
    ]))

    CASTS_SCHEMA = ArrayType(StructType([
        StructField("cast_id", IntegerType()),
        StructField("character", StringType()),
        StructField("credit_id", StringType()),
        StructField("gender", IntegerType()),
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("order", StringType()),
        StructField("profile_path", StringType())
    ]))

    CREDITS_SCHEMA = ArrayType(StructType([
        StructField("department", StringType()),
        StructField("credit_id", StringType()),
        StructField("gender", IntegerType()),
        StructField("id", IntegerType()),
        StructField("job", StringType()),
        StructField("name", StringType()),
        StructField("profile_path", StringType())
    ]))

    COLLECTIONS_SCHEMA = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("poster_path", StringType()),
        StructField("backdrop_path", StringType())
    ])

    def __init__(self, base_s3_destination):
        """Instantiates object"""
        if base_s3_destination.endswith("/"):
            self.base_s3_destination = base_s3_destination + "{name}/"
        else:
            self.base_s3_destination = base_s3_destination + "/{name}/"

    def process_all_files(self, credits_s3_source, keywords_s3_source, links_s3_source,
                          links_small_s3_source, ratings_s3_source, ratings_small_s3_source,
                          movies_s3_source):
        """Main process to process all movie database files and transform them into
        new schema in s3 destination

        Args:
            credits_s3_source: s3 source where credits.csv file is located
            keywords_s3_source: s3 source where keywords.csv file is located
            links_s3_source: s3 source where links.csv file is located
            links_small_s3_source: s3 source where links_small.csv file is located
            ratings_s3_source: s3 source where ratings.csv file is located
            ratings_small_s3_source: s3 source where ratings_small.csv file is located
            movies_s3_source: s3 source where movies_metadata.csv file is located

        Returns:
            None

        """
        self.process_credits_file(credits_s3_source)
        self.process_keywords_file(keywords_s3_source)
        self.process_links_file(links_s3_source)
        self.process_links_file(links_small_s3_source)
        self.process_ratings_file(ratings_s3_source)
        self.process_ratings_file(ratings_small_s3_source)
        self.process_movies_file(movies_s3_source)

    def process_credits_file(self, s3_source):
        """Process movies database credits.csv and transform into new schema

        Args:
            s3_source: s3 source where credits.csv file is located

        Returns:
            None

        """
        df = self._read_df_csv(source=s3_source)

        # Instantiate preliminary dataframes
        casts_df = df.select(
                col("id"),
                explode(from_json(col("cast"),
                        self.CASTS_SCHEMA)).alias("casts_array"))

        crews_df = df.select(
                col("id"),
                explode(from_json(col("crew"),
                        self.CREDITS_SCHEMA)).alias("crews_array"))

        # Create final dataframes for writing
        final_movie_characters_df = casts_df.select(
                col("id").cast(IntegerType()).alias("movie_id"),
                col("casts_array")["cast_id"].alias("cast_id"),
                col("casts_array")["order"].alias("cast_order"),
                col("casts_array")["character"].alias("character"),
                col("casts_array")["gender"].alias("gender"),
                col("casts_array")["id"].alias("actor_id"))\
            .filter(col("id").rlike('^([0-9]*\,?[0-9]+|[0-9]+\,?[0-9]*)?$'))\
            .orderBy("id")\
            .repartition(1)

        final_actors_df = casts_df.select(
                col("casts_array")["id"].alias("id"),
                col("casts_array")["name"].alias("name"),
                col("casts_array")["profile_path"].alias("profile_path"))\
            .filter(col("casts_array")["id"].rlike('^([0-9]*\,?[0-9]+|[0-9]+\,?[0-9]*)?$'))\
            .distinct()\
            .orderBy(col("casts_array")["id"])\
            .repartition(1)

        final_movie_crews_df = crews_df.select(
                col("id").cast(IntegerType()).alias("movie_id"),
                col("crews_array")["department"].alias("department"),
                col("crews_array")["credit_id"].alias("credit_id"),
                col("crews_array")["id"].alias("crew_id"))\
            .filter(col("id").rlike('^([0-9]*\,?[0-9]+|[0-9]+\,?[0-9]*)?$'))\
            .orderBy("id")\
            .repartition(1)

        final_crew_jobs_df = crews_df.select(
                col("crews_array")["id"].alias("id"),
                col("crews_array")["name"].alias("name"),
                col("crews_array")["profile_path"].alias("profile_path"))\
            .filter(col("crews_array")["id"].rlike('^([0-9]*\,?[0-9]+|[0-9]+\,?[0-9]*)?$'))\
            .distinct()\
            .orderBy(col("crews_array")["id"])\
            .repartition(1)

        # Write dataframes to s3 destinations
        self._write_df_json(final_movie_characters_df,
                            self.base_s3_destination.format(
                                name="movie_characters"))
        self._write_df_json(final_actors_df,
                            self.base_s3_destination.format(name="actors"))
        self._write_df_json(final_movie_crews_df,
                            self.base_s3_destination.format(name="movie_crews"))
        self._write_df_json(final_crew_jobs_df,
                            self.base_s3_destination.format(name="crews"))

    def process_keywords_file(self, s3_source):
        """Process movies database keywords.csv and transform into new schema

        Args:
            s3_source: s3 source where keywords.csv file is located

        Returns:
            None

        """
        df = self._read_df_csv(s3_source)

        # Create intermediate dataframe
        transformed_df = df.select(
                col("id"),
                explode(from_json(col("keywords"),
                        self.COMMON_BASE_SCHEMA)).alias("keywords_array"))

        # Create final dataframes for writing
        final_keywords_df = transformed_df.select(
                col("keywords_array")["id"].alias("id"),
                col("keywords_array")["name"].alias("name"))\
            .distinct()\
            .orderBy(col("id"))\
            .repartition(1)

        final_keyword_movies_df = transformed_df.select(
                col("id").cast(IntegerType()).alias("movie_id"),
                col("keywords_array")["id"].alias("keyword_id"))\
            .orderBy(col("id"))\
            .repartition(1)

        # Write dataframes to s3 destinations
        self._write_df_json(final_keywords_df,
                            self.base_s3_destination.format(name="keywords"))
        self._write_df_json(final_keyword_movies_df,
                            self.base_s3_destination.format(name="movie_keywords"))

    # Write dataframes to s3 destinations
    def process_links_file(self, s3_source):
        """Process movies database links.csv or links_small.csv and
        transform into new schema

        Args:
            s3_source: s3 source where links/links_small.csv file is located

        Returns:
            None

        """
        df = self._read_df_csv(s3_source)

        # Create final dataframe for writing
        final_df = df.select(
                col("movieId").cast(IntegerType()).alias("movie_id"),
                col("imdbId").alias("imdb_id"),
                col("tmdbId").cast(IntegerType()).alias("tmdb_id"))\
            .repartition(1)

        # Is the source file links.csv or links_small.csv?
        if s3_source.endswith("links.csv"):
            name = "links"
        else:
            name = "links_small"

        # Write dataframes to s3 destinations
        self._write_df_json(final_df,
                            self.base_s3_destination.format(name=name))

    def process_ratings_file(self, s3_source):
        """Process movies database ratings.csv or ratings_small.csv and
        transform into new schema

        Args:
            s3_source: s3 source where ratings/ratings_small.csv file is located

        Returns:
            None

        """
        df = self._read_df_csv(s3_source)

        # Create final dataframes for writing
        final_df = df.select(
                col("userId").cast(IntegerType()).alias("user_id"),
                col("movieId").cast(IntegerType()).alias("movie_id"),
                col("rating").cast(FloatType()).alias("rating"),
                col("timestamp").cast(TimestampType()).alias("timestamp"))\
            .repartition(1)

        # Is the source file ratings.csv or ratings_small.csv?
        if s3_source.endswith("ratings.csv"):
            name = "ratings"
        else:
            name = "ratings_small"

        # Write dataframes to s3 destinations
        self._write_df_json(final_df,
                            self.base_s3_destination.format(name=name))

    def process_movies_file(self, s3_source):
        """Process movies database movies_metadata.csv or ratings_small.csv and
        transform into new schema

        Args:
            s3_source: s3 source where movies_metadata.csv file is located

        Returns:
            None

        """
        df = self._read_df_csv(s3_source)

        # Create intermediate dataframes
        collections_df = df.select(
                from_json(col("belongs_to_collection"),
                          self.COLLECTIONS_SCHEMA).alias("collections_array"))\
            .distinct()

        genres_df = df.select(
                explode(from_json(col("genres"),
                        self.COMMON_BASE_SCHEMA)).alias("genres_array"))\
            .distinct()

        production_companies_df = df.select(
                explode(from_json(col("production_companies"),
                        self.COMMON_BASE_SCHEMA)).alias("production_companies_array"))\
            .distinct()

        # Create final dataframes for writing
        final_genres_df = genres_df.select(
                col('genres_array')["id"].alias("id"),
                col('genres_array')["name"].alias("name"))\
            .orderBy(col("id"))\
            .repartition(1)

        final_collections_df = collections_df.select(
                col('collections_array')["id"].alias("id"),
                col('collections_array')["name"].alias("name"),
                col('collections_array')["poster_path"].alias("poster_path"),
                col('collections_array')["backdrop_path"].alias("backdrop_path"))\
            .orderBy(col("id")) \
            .repartition(1)

        final_production_companies_df = production_companies_df.select(
                col('production_companies_array')["id"].alias("id"),
                col('production_companies_array')["name"].alias("name"))\
            .orderBy(col("id"))\
            .repartition(1)

        final_movies_collections_df = df.select(
                col("id").cast(IntegerType()).alias("movie_id"),
                from_json(col("belongs_to_collection"),
                          self.COLLECTIONS_SCHEMA)["id"].alias("collection_id"))\
            .orderBy(col("movie_id"))\
            .repartition(1)

        final_movies_genres_df = df.select(
                col("id").cast(IntegerType()).alias("movie_id"),
                explode(from_json(col("genres"),
                        self.COMMON_BASE_SCHEMA)["id"]).alias("genre_id"))\
            .orderBy(col("movie_id"))\
            .repartition(1)

        final_movie_production_companies_df = df.select(
                col("id").cast(IntegerType()).alias("movie_id"),
                explode(from_json(col("production_companies"),
                        self.COMMON_BASE_SCHEMA)["id"]).alias("production_company_id"))\
            .orderBy(col("movie_id"))\
            .repartition(1)

        final_movies_df = df.select(
                col("id").cast(IntegerType()).alias("id"),
                col("adult").alias("adult"),
                col("budget").cast(FloatType()).cast(IntegerType()).alias("budget"),
                col("homepage").alias("homepage"),
                col("imdb_id").alias("imdb_id"),
                col("original_language").alias("original_language"),
                col("original_title").alias("original_title"),
                col("overview").alias("overview"),
                col("popularity").alias("popularity"),
                col("poster_path").alias("poster_path"),
                col("production_countries").alias("production_countries"),
                col("release_date").alias("release_date"),
                col("revenue").cast(FloatType()).alias("revenue"),
                col("runtime").cast(IntegerType()).alias("runtime"),
                col("spoken_languages").alias("spoken_languages"),
                col("status").alias("status"),
                col("tagline").alias("tagline"),
                col("title").alias("title"),
                col("video").alias("video"),
                col("vote_average").cast(FloatType()).alias("vote_average"),
                col("vote_count").cast(IntegerType()).alias("vote_count"))\
            .orderBy(col("id"))\
            .repartition(1)

        # Write dataframes to s3 destinations
        self._write_df_json(final_genres_df,
                            self.base_s3_destination.format(name="genres"))
        self._write_df_json(final_collections_df,
                            self.base_s3_destination.format(name="collections"))
        self._write_df_json(final_production_companies_df,
                            self.base_s3_destination.format(
                                name="production_companies"))
        self._write_df_json(final_movies_collections_df,
                            self.base_s3_destination.format(
                                name="movie_collections"))
        self._write_df_json(final_movies_genres_df,
                            self.base_s3_destination.format(
                                name="movie_genres"))
        self._write_df_json(final_movie_production_companies_df,
                            self.base_s3_destination.format(
                                name="movie_production_companies"))
        self._write_df_json(final_movies_collections_df,
                            self.base_s3_destination.format(
                                name="movie_collections"))
        self._write_df_json(final_movies_df,
                            self.base_s3_destination.format(
                                name="movies"))

    def _read_df_csv(self, source):
        """Helper method to read csv source file from s3

        Args:
            source: s3 source where file is located.

        Returns:
            Dataframe

        """
        return self.SPARK.read\
            .option("header", "true")\
            .csv(source)

    @staticmethod
    def _write_df_json(df, destination):
        """Helper method to write dataframe to json to s3 destination

        Args:
            destination: s3 destination where dataframe is being written

        Returns:
            None

        """
        df.write\
            .option("compression", "gzip")\
            .mode("overwrite")\
            .json(destination)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: MovieDataProcessor  ", file=sys.stderr)
        exit(-1)

    movie_data_processor = MovieDataProcessor(sys.argv[1])

    movie_data_processor.process_all_files(
        credits_s3_source="s3a://guild-test-raw/movie-data/credits.csv",
        keywords_s3_source="s3a://guild-test-raw/movie-data/keywords.csv",
        links_s3_source="s3a://guild-test-raw/movie-data/links.csv",
        links_small_s3_source="s3a://guild-test-raw/movie-data/links_small.csv",
        ratings_s3_source="s3a://guild-test-raw/movie-data/ratings.csv",
        ratings_small_s3_source="s3a://guild-test-raw/movie-data/ratings_small.csv",
        movies_s3_source="s3a://guild-test-raw/movie-data/movies_metadata.csv")