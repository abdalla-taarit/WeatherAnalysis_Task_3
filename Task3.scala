package com.sundogsoftware.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{to_date, to_timestamp}
object Task3 {

  def AnswerandSave(answer :DataFrame,question : String)={
    answer.coalesce(1).write.format(question+".csv")
      .option("header","true").mode("overwrite")
      .csv(question)
  }


  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("task").master("local[*]").getOrCreate()

    import spark.implicits._

    val WeatherSchema = new StructType()
      .add("Date", StringType, nullable = true)
      //.add("Date", DateType, nullable = true)
      .add("Location", StringType, nullable = true)
      .add("MinTemp", FloatType, nullable = true)
      .add("MaxTemp", FloatType, nullable = true)
      .add("Rainfall", FloatType, nullable = true)
      .add("Evaporation", FloatType, nullable = true)
      .add("Sunshine", FloatType, nullable = true)
      .add("WindGustDir", StringType, nullable = true)
      .add("WindGustSpeed", IntegerType, nullable = true)
      .add("WindDir9am", StringType, nullable = true)
      .add("WindDir3pm", StringType, nullable = true)
      .add("WindSpeed9am", FloatType, nullable = true)
      .add("WindSpeed3pm", FloatType, nullable = true)
      .add("Humidity9am", FloatType, nullable = true)
      .add("Humidity3pm", FloatType, nullable = true)
      .add("Pressure9am", FloatType, nullable = true)
      .add("Pressure3pm", FloatType, nullable = true)
      .add("Cloud9am", IntegerType, nullable = true)
      .add("Cloud3pm", IntegerType, nullable = true)
      .add("Temp9am", FloatType, nullable = true)
      .add("Temp3pm", FloatType, nullable = true)
      .add("RainToday", StringType, nullable = true)
      .add("RainTomorrow",StringType, nullable = true)

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val df_raw = spark.read.format("csv").option("header","true").schema(WeatherSchema)
      .load("weatherAUS.csv")
      .select("Date","Location","MinTemp","MaxTemp",
        "WindGustSpeed","Pressure9am","Pressure3pm","RainToday")
      .withColumn("Datecolumn", to_date($"Date", "MM/dd/yyyy"))


    //for quering and answering questions
    val df_fixed = df_raw.na.drop("any")

    val df_final = df_fixed.withColumn("Date",split($"Date", "/"))
      .withColumn("year", $"Date"(2).cast(IntegerType))
      .withColumn("month", $"Date"(0).cast(IntegerType))
      .withColumn("Pressure",(($"Pressure9am" + $"Pressure3pm")/2).cast(FloatType))
      .drop("Date")
      .drop("Pressure9am")
      .drop("Pressure3pm")


    df_final.printSchema()
    df_final.show()
    AnswerandSave(df_final,"FixedTable")
//------------------------------------------------------------------------------------------------------
    val win = Window.partitionBy($"Location").orderBy($"Datecolumn").
    rowsBetween(0, Window.unboundedFollowing)

    val df_answer = df_final.
      withColumn("RainDate", when($"RainToday".equalTo("Yes"), $"Datecolumn"))

    df_answer.createOrReplaceTempView("data")

    val df_answer_2 = df_final.
      withColumn("RainDate", when($"RainToday".equalTo("Yes"), $"Datecolumn"))
          .withColumn("interval",
            datediff(first($"RainDate", ignoreNulls=true).over(win), $"Datecolumn")
          )

    AnswerandSave(    df_answer_2.select($"Location", $"Datecolumn".as("start_date"),
      max($"interval").over(Window.partitionBy($"Location")).as("max_interval")
    ).where($"interval" === $"max_interval"),"find_max_range_withnoRain")

    val df_answer1 = spark.sql("Select Location,Datecolumn,RainDate," +
      "Datediff(First(RainDate,true) Over(Partition By Location Order By Datecolumn" +
      " ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) ,Datecolumn) As i" +
      " From data")

    //df_answer1.printSchema()
    df_answer1.createOrReplaceTempView("table")
    val df_answer2 = spark.sql("Select Location,max(i) As range From table Group By Location Order By range Desc")
    AnswerandSave(df_answer2,"SQLfind_maxrange_of_no_rain_perloc")

    var min_max_avgTemp = df_final.select("year","Location","MinTemp","MaxTemp")
      .groupBy("Location","year").agg(min("MinTemp")
      .as("MinTemp"),max("MaxTemp")
      .as("MaxTemp"),round(avg("MinTemp"),2)
      .as("avgMin_Temp_per_year"),round(avg("MaxTemp"),2)
      .as("avgMax_Temp_per_year"),max(round(($"MaxTemp" + $"MinTemp")/2,2))
      .as("highest_avg_temp_inday_peryear"),avg(($"MaxTemp" + $"MinTemp")/2).as("avg_temp_peryear"))
      .sort("year")

    var SQLmin_max_avgTemp =
      spark.sql("Select data.Location,data.year,Min(data.MinTemp) As MinTemp,Max(data.MaxTemp) As MaxTemp " +
      ",Round(avg(data.MinTemp),2) As avgMin_Temp_per_year,Round(avg(data.MaxTemp),2) As avgMax_Temp_per_year" +
        ",max(Round((data.MaxTemp + data.MinTemp)/2,2)) As highest_avg_temp_inday_peryear" +
        ",avg((data.MaxTemp+data.MinTemp)/2) As avg_temp_peryear From data Group By Location,year Order By year")

      AnswerandSave(SQLmin_max_avgTemp,"SQLfindminmaxavg_Temp_PerLoc_Peryear")
    AnswerandSave(min_max_avgTemp,"findminmaxavg_Temp_PerLoc_Peryear")

    var min_max_avgPressure = df_final.select("year","Location","Pressure")
      .groupBy("Location","year").agg(min("Pressure")
      .as("MinPressure"),max("Pressure")
      .as("MaxPressure"),round(avg("Pressure"),2)
      .as("avg_Pressure_per_year"))
      .sort("year")

    var SQLmin_max_avgPressure =
      spark.sql("Select data.Location,data.year,Min(data.Pressure) As MinPressure,Max(data.Pressure) As MaxPressure " +
  ",Round(avg(data.Pressure),2) As avg_Pressure_per_year From data Group By Location,year Order By year")

      AnswerandSave(SQLmin_max_avgPressure,"SQLfindMin_Max_AvgPressure_PerCity_PerYear")
    AnswerandSave(min_max_avgPressure,"findMin_Max_AvgPressure_PerCity_PerYear")

    var min_max_avgWind = df_final.select("year","Location","WindGustSpeed")
      .groupBy("Location","year").agg(min("WindGustSpeed")
      .as("MinWindSpeed"),max("WindGustSpeed")
      .as("MaxWindSpeed"),round(avg("WindGustSpeed"),2)
      .as("avgWindSpeed")).sort("year")

    var SQLmin_max_avgWind =
    spark.sql("Select data.Location,data.year,Min(data.WindGustSpeed) As MinWindSpeed,Max(data.WindGustSpeed) As MaxWindSpeed " +
    ",Round(avg(data.WindGustSpeed),2) As avg_WindSpeed_per_year From data Group By Location,year Order By year")

    AnswerandSave(SQLmin_max_avgWind,"SQLfindMin_Max_AvgWindSpeed_PerCity_PerYear")

    AnswerandSave(min_max_avgWind,"findMin_Max_AvgWindSpeed_PerCity_PerYear")



    spark.stop()


  }
}
