package com.mapr.sparkOjaiApp

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.types.ODate
import com.mapr.db.spark._

object SparkOjaiApplication {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("json app").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val allUsers = sc.parallelize(getUsers())
    allUsers.saveToMapRDB("/tmp/UserInfo", createTable = true)

    //get all the people who
    val allUsersInfo = sc.loadFromMapRDB("/tmp/UserInfo")
    assert(allUsersInfo.count == 5)
    val names = allUsersInfo.map(person => person.first_name[String]).collect
    assert(names.toSet.equals(Set("David", "Peter", "James", "Jimmy", "Indiana")))
    val info = allUsersInfo.collect

    info.foreach(println(_))

    val usersLivingInMilpitas = sc.loadFromMapRDB("/tmp/UserInfo").where(field("address.city") === "milpitas").collect
    usersLivingInMilpitas.foreach(println(_))

    val usersName = sc.loadFromMapRDB("/tmp/UserInfo").select("first_name", "last_name")
    usersName.foreach(println(_))
  }

  def getUsers(): Array[Person] = {
    val users: Array[Person] = Array(
      Person("DavUSCalif", "David", "Jones", ODate.parse("1947-11-29"), Seq("football", "books", "movies"), Map("city" -> "milpitas", "street" -> "350 holger way", "Pin" -> 95035)),
      Person("PetUSUtah", "Peter", "pan", ODate.parse("1974-1-29"), Seq("boxing", "music", "movies"), Map("city" -> "salt lake", "street" -> "351 lake way", "Pin" -> 89898)),
      Person("JamUSAriz", "James", "junior", ODate.parse("1968-10-2"), Seq("tennis", "painting", "music"), Map("city" -> "phoenix", "street" -> "358 pond way", "Pin" -> 67765)),
      Person("JimUSCalif", "Jimmy", "gill", ODate.parse("1976-1-9"), Seq("cricket", "sketching"), Map("city" -> "san jose", "street" -> "305 city way", "Pin" -> 95652)),
      Person("IndUSCalif", "Indiana", "Jones", ODate.parse("1987-5-4"), Seq("squash", "comics", "movies"), Map("city" -> "sunnyvale", "street" -> "35 town way", "Pin" -> 95985))
    )
    users
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class Person (@JsonProperty("_id") id: String, @JsonProperty("first_name") firstName:String,
                 @JsonProperty("last_name") lastName: String, @JsonProperty("dob") dob: ODate,
                 @JsonProperty("interests") interests: Seq[String], @JsonProperty("address") address: Map[String, Any])
