
# Overview
Solving analytical questions on the semi-structured [MovieLens dataset](https://grouplens.org/datasets/movielens/1m/) containing a million records using Spark and Scala. This features the use of Spark RDD, Spark SQL and Spark Dataframes executed on Spark-Shell (REPL) using Scala API. We aim to draw useful insights about users and movies by leveraging different forms of Spark APIs.

# Major Components

<p align="center">
	<a href="#">
		<img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" alt="Apache Spark Logo" title="Apache Spark" width=275 hspace=80 />
	</a>
	<a href="#">
		<img src="https://raw.githubusercontent.com/Thomas-George-T/Thomas-George-T/master/assets/scala.svg" alt="Scala" title="Scala" width ="90" />
	</a>
</p>

# Analytical Queries

## Spark RDD
- [What are the top 10 most viewed movies?](/src/main/scala/RDDs/MostViewedMovies.scala)
- [What are the distinct list of genres available?](/src/main/scala/RDDs/DistinctGenres.scala)
- [How many movies for each genre?](/src/main/scala/RDDs/MoviesPerGenre.scala)
- [How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?](/src/main/scala/RDDs/StartingWithCharacter.scala)
- [List the latest released movies](/src/main/scala/RDDs/LatestMovies.scala)

## Spark SQL
- [Create tables for movies.dat, users.dat and ratings.dat](/src/main/scala/SQL/SaveAsTable.scala)
- [Find the list of the oldest released movies.](/src/main/scala/SQL/OldestReleasedMovies.scala)
- [How many movies are released each year?](/src/main/scala/SQL/MoviesPerYear.scala)
- [How many number of movies are there for each rating?](/src/main/scala/SQL/MoviesForEachRating.scala)
- [How many users have rated each movie?](/src/main/scala/SQL/UserRatingEachMovie.scala)
- [What is the total rating for each movie?](/src/main/scala/SQL/TotalRatingForEachMovie.scala)
- [What is the average rating for each movie?](/src/main/scala/SQL/AverageRatingForEachMovie.scala)

## Spark DataFrames
- [Prepare Movies data: Extracting the Year and Genre from the Text](/src/main/scala/dataFrames/Prepare_Movies_Data.scala)
- [Prepare Users data: Loading a double delimited csv file](/src/main/scala/dataFrames/Prepare_Ratings_Data.scala)
- [Prepare Ratings data: Programmatically specifying a schema for the dataframe](/src/main/scala/dataFrames/Prepare_Users_Data.scala)


