package szeged.redSteed.test

import szeged.redSteed.closeSpark
import szeged.redSteed.getLocalSparkContext
import szeged.redSteed.measureTimeInMillis


class RunSpark {
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val time = measureTimeInMillis {
                val list = listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                val jsc = getLocalSparkContext("reed steed test")
                val listRDD = jsc.parallelize(list)

                val sum = listRDD.reduce({ a, b -> a + b })
                println(sum)
                closeSpark(jsc)
            }
            println("Execution time was ${time.second}")
        }

    }
}