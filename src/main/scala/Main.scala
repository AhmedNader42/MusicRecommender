package org.ahmed.MusicRecommender
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val spark = SparkSession.builder().getOrCreate()
    val DATA_PATH = "/home/ahmed/Documents/github/spark-projects/profiledata_06-May-2005"

    val rawArtistData = spark.read.textFile(DATA_PATH + "/user_artist_data.txt")
  }
}