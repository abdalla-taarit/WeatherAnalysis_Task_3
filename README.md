# WeatherAnalysis
* [General info](#general-info)
* [Technologies](#technologies)
* [Dataset Description](#dataset-description)
* [Data Dictionary](#data-dictionary)
* [SQl Queries](#sql-queries)
* [Setup](#setup)

## General info
This project aim is to get your hands dirty using sparksql on some dataset fo your own choice and to visualize your results. Both imperative and declerative query styles were used.
	
## Technologies
Project is created with:
* IDE IntelliJ
* JVM: 8.0
* Scala version: 2.12.12
* Spark version: 3.0.0

## [Dataset Description](https://www.kaggle.com/jsphyg/weather-dataset-rattle-package)
The dataset is called Rain in Australia Dataset. This dataset contains about 10 years of daily weather observations from many locations across Australia. The original purpose of this dataset was to predict the rainTomrrow.The dataset was used for trying to find patterns in how pressure,Temperature and wind speed changed over the years. It is also to see if these changes are shared by different locations in Australian or not.

The main Answers I was looking for were:
1. Find the Max,Min,avgMax, avgMin temperature and highest average temperature per year given location and city then compare it with other cities.This will show how Temperature over cities changed over years and could give a hint about a trend.
2. Find the Max,Min and avgPressure per year given location and city then compare it with other cities.The same as the first answer but deals with pressure.
3. Find the Max,Min,avgWindSpeed per year given location and city then compare it with other cities.The same with the first answer but deals with wind speed.
4. What's the longest period of no rain for each city?

## Data Dictionary
The following table shows the fields that were used from the csv file for quering and analysis, and their descriptions.
|Field name | Datatype |Description
| --- | --- | --- |
|Location | String | Shows location where data was captured |
| MinTemp | float |Minimum temperature / day |
|MaxTemp | float | Maximum Temperature of/ day |
|Wind gust speed | float | Avg Wind speed/ day |
|Rain Today | String | If rain occurred today or not |
|Date column | Date | Date of data capture |
|year | Integer |Shows the year in which this data was captured |
|month |Integer | Shows the month in which this data was captured |
|Pressure | float | Average pressure captured this day |

## SQl Queries
#### Imperative
1. find the longest period of no rain for each city
```
    val win = Window.partitionBy($"Location").orderBy($"Datecolumn").
        rowsBetween(0, Window.unboundedFollowing)
    
    val df_answer = df_final.
        withColumn("RainDate", when($"RainToday".equalTo("Yes"),$"Datecolumn"))
          .withColumn("interval",
            datediff(first($"RainDate", ignoreNulls=true)
            .over(win), $"Datecolumn"))
            
    df_answer.select($"Location", $"Datecolumn".as("start_date"),
        max($"interval").over(Window.partitionBy($"Location")).as("max_interval")
        ).where($"interval" === $"max_interval")
```
2. find the longest period of no rain for each city

#### Declerative

	
## Setup
To run this project, just run the jar file and make sure weather.csv is present in your same directry.