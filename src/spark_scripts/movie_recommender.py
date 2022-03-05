from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

scoreThreshold: float = 0.0
coOccurrenceThreshold: int = 0
movieID: int = 0

if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    movieID = int(sys.argv[1])
else:
    sys.exit(-1)


def compute_cosine_similarity(data, target_movie_id):
    # Compute xx, xy and yy columns
    pair_scores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2")) \
        .filter(((func.col("movie1") != target_movie_id) & (func.col("rating1") >= 4)) | (
            (func.col("movie2") != target_movie_id) & (func.col("rating2") >= 4)))

    # Compute numerator, denominator and numPairs columns
    calculate_similarity = pair_scores \
        .groupBy("movie1", "movie2") \
        .agg(func.sum(func.col("xy")).alias("numerator"),
             (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
             func.count(func.col("xy")).alias("numPairs")
             )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    similar_movies = calculate_similarity \
        .withColumn("score",
                    func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator"))
                    .otherwise(0)
                    ).select("movie1", "movie2", "score", "numPairs")

    return similar_movies


# Get movie name by given movie id
def get_movie_name(movie_names, movie_id):
    movie_info = movie_names.filter(func.col("movieID") == movie_id) \
        .select("movieTitle").collect()[0]

    return movie_info[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

moviesSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])

# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv(f"file://{ROOT_DIR}/data/ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
    .option("sep", "\t") \
    .schema(moviesSchema) \
    .csv(f"file://{ROOT_DIR}/data/ml-100k/u.data")

ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
          & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
    .select(func.col("ratings1.movieId").alias("movie1"),
            func.col("ratings2.movieId").alias("movie2"),
            func.col("ratings1.rating").alias("rating1"),
            func.col("ratings2.rating").alias("rating2"))

moviePairSimilarities = compute_cosine_similarity(moviePairs, movieID).cache()

if __name__ == '__main__':
    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
        (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + get_movie_name(movieNames, movieID))

    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(get_movie_name(movieNames, similarMovieID) + "\tscore: " + str(result.score) + "\tstrength: " + str(
            result.numPairs))
