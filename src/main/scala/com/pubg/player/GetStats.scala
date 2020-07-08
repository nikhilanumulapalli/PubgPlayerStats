package com.pubg.player

import com.pubg.player.Details.{filterData, getKill}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object GetStats {
  def setupLogging() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def main(args: Array[String]): Unit = {
    setupLogging()

    val sc = new SparkContext("local[*]" , "FindMatch")
    val data = sc.textFile("./data/deaths/*")

    // read the header to skip it in the data
    val header = data.first

    // filter data by skipping header and rows with null values
    val new_data = data.filter(row => row != header & filterData(row))

    // extract data from string rows
    val killsRdd = new_data.map(record => getKill(record))

    // making (playerName, matchId, rank) as key to add killDistance and killTime
    val matchStats = killsRdd.map{case (name, matchId, time, killDistance, rank) =>
      ((name, matchId, rank), (time, killDistance, 1))}.reduceByKey((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3))

    // filtering player records with more than one kill per match
    val moreThanAKillInMatch = matchStats.filter(_._2._3 > 1)

    // calculating rank, time, distance per kill in match along with number of kills
    // making playerName as key with rest of statics as value
    val perKillInMatch = matchStats.map{case (a, b) => (a, (b._1/b._3, b._2/b._3, b._3))}
      .map{case (a, b) => (a._1, (a._3, b._1, b._2, b._3, 1))}

    // (playerName, (rankSum, totalAvgTimePerKill, totalAvgDistancePerKill, totalKills)
    val playerTotalStats = perKillInMatch.reduceByKey((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3, a._4+b._4, a._5+b._5))

    // playerName, rankPerMatch, timePerMatch, killDistancePerMatch, killsPerMatch
    val playerPerKillPerMatch = playerTotalStats.map{case (a, b) => (a, (b._1/b._5, b._2/b._5, b._3/b._5, b._4/b._5))}
  }
}

