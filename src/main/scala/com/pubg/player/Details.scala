package com.pubg.player

import scala.math.{pow, sqrt}

object Details {
  def filterData(record: String): Boolean ={
    val values = record.split(",")
    val isValueNull = values.map(value => value != "")
    isValueNull.reduce(_ & _)
  }
  def getKill(killerName: String, killerRank: Int, matchId: String, time: Int, killerX: Double, killerY: Double, victimX: Double, victimY: Double): (String, String, Int, Double, Int) = {
    val distance = sqrt(pow(killerX - victimX, 2) + pow(killerY - victimY, 2))
    (killerName, matchId, time, distance, killerRank)
  }

  def getKill(record: String): (String, String, Int, Double, Int) = {
    val values = record.split(",")
    getKill(values(1), values(2).toDouble.toInt, values(6), values(7).toInt, values(3).toDouble, values(4).toDouble, values(10).toDouble, values(11).toDouble)
  }
}
