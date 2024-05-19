package org.ahmed.MusicRecommender

import org.apache.spark.broadcast._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("MusicRecommender").getOrCreate()
    import spark.implicits._

    val DATA_PATH = "/home/ahmed/Documents/github/spark-projects/profiledata_06-May-2005"

    val rawUserArtistData = spark.read.textFile(DATA_PATH + "/user_artist_data.txt")

    //    rawArtistData.take(5).foreach(println)

    val userArtistDF = rawUserArtistData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")
    userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

    val rawArtistData = spark.read.textFile(DATA_PATH + "/artist_data.txt")
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")

    val rawArtistAlias = spark.read.textFile(DATA_PATH + "/artist_alias.txt")
    val artistAlias = rawArtistAlias.flatMap { line =>
      val Array(artist, alias) = line.split('\t')
      if (artist.isEmpty) {
        None
      } else {
        Some((artist.toInt, alias.toInt))
      }
    }.collect().toMap

    artistByID.filter($"id" isin(1208690, 1003926)).show()

    def buildCounts(rawUserArtistData: Dataset[String], bArtistAlias: Broadcast[Map[Int, Int]]): DataFrame = {
      rawUserArtistData.map { line =>
        val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
        (userID, finalArtistID, count)
      }.toDF("user", "artist", "count")
    }

    val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
    val trainData = buildCounts(rawUserArtistData, bArtistAlias)
    trainData.cache()

    val model = new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      .setRank(10)
      .setRegParam(0.01)
      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(trainData)

    model.userFactors.show(1, truncate = false)

    def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
      val toRecommend = model.itemFactors.select($"id".as("artist")).withColumn("user", lit(userID))

      model.transform(toRecommend).select("artist", "prediction").orderBy($"prediction".desc).limit(howMany)
    }

    val userID = 2093760
    val topRecommendations = makeRecommendations(model, userID, 5)
    val recommendedArtistID = topRecommendations.select("artist").as[Int].collect()
    artistByID.filter($"id" isin (recommendedArtistID: _*)).show()
  }
}