package szeged.redSteed.task1

import scala.Tuple2
import szeged.redSteed.*


class Task1 {
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val time = measureTimeInMillis {
                val task1 = Task1()
                val file1 = "user_test_30000.csv"
                val file2 = "user_test_100000.csv"
                task1.runTask1("./data/$file2", isSaveResults = true, isDebug = true)
            }
            println("Execution time was ${time.second}")
        }


    }

    fun runTask1(dataFileName: String, isSaveResults: Boolean, isDebug: Boolean) {
        val jsc = getLocalSparkContext("task1")

        val data = jsc.textFile(dataFileName)

        val noErrorData = data.filter({ line ->
            val splittedLine = line.split(",")
            splittedLine.size > 3 && splittedLine.last() != "error"
        }).mapToPair { line ->
            val splittedLine = line.split(",")
            Tuple2(splittedLine[0], Tuple2(splittedLine[2], splittedLine[3]))
        }

        if (isDebug) {
            println("number of elements: ${noErrorData.count()}")
        }


        val categoriesFreq = jsc.broadcast(noErrorData.mapToPair { data ->
            Tuple2(data._2._2, 1L)
        }.reduceByKey({ a, b -> a + b }).collectAsMap())


        val userActivities = noErrorData.combineByKeyIntoList()


        val userActivitiesFormatted = userActivities.mapToPair { userActivity ->
            val (userId, userEvents) = userActivity
            val days = userEvents.map { userEvents -> userEvents._1 }
            val userCategories = userEvents.map { userEvents -> userEvents._2 }
            Tuple2(userId, Tuple2(days, userCategories))
        }


        val userActivitiesCount = userActivities.flatMapToPair { userActivity ->
            userActivity._2.map { userEvent -> Tuple2("${userActivity._1}@${userEvent._2}", 1L) }.iterator()
        }.reduceByKey({ a, b -> a + b }).reduceSplit().combineByKeyIntoList()


        val normalizedUserActivityCount = userActivitiesCount.mapToPair { userActivityCount ->
            val (userID, userEventsCount) = userActivityCount

            val normalizedUserEventCount = userEventsCount.map { userEventCount ->
                val (category, eventFreq) = userEventCount
                val normalizedFreq = eventFreq / (categoriesFreq.value[category]?.toDouble() ?: 0.0)
                //Tuple2(category, normalizedFreq)
                normalizedFreq
            }

            Tuple2(userID, normalizedUserEventCount)
        }

        if (isSaveResults) {
            val userActivitiesResultsFileName = "./data/results/userActivitiesResults/${dataFileName.split("/").last()}"
            deleteFileIfExist(userActivitiesResultsFileName)
            userActivitiesFormatted.saveAsTextFile(userActivitiesResultsFileName)

            val userActivitiesCategoryFreqResultsFileName = "./data/results/userCategoryFreqsResults/${dataFileName.split("/").last()}"
            deleteFileIfExist(userActivitiesCategoryFreqResultsFileName)
            userActivitiesCount.saveAsTextFile(userActivitiesCategoryFreqResultsFileName)

            val normalizedUserActivitiesCategoryFreqResultsFileName = "./data/results/userCategoryNormFreqsResults/${dataFileName.split("/").last()}"
            deleteFileIfExist(normalizedUserActivitiesCategoryFreqResultsFileName)
            normalizedUserActivityCount.saveAsTextFile(normalizedUserActivitiesCategoryFreqResultsFileName)

        }

        if (isDebug) {
            println(normalizedUserActivityCount.take(10).joinToString("\n"))
        }


        closeSpark(jsc)
    }
}

