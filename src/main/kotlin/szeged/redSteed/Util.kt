package szeged.redSteed

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.Function2
import scala.Tuple2
import java.io.File


operator fun <T1, T2> Tuple2<T1, T2>.component1(): T1 = this._1
operator fun <T1, T2> Tuple2<T1, T2>.component2(): T2 = this._2
fun <T1, T2> Tuple2<T1, T2>.pair(): Pair<T1, T2> = Pair(this._1, this._2)
fun <T1, T2> Pair<T1, T2>.tuple(): Tuple2<T1, T2> = Tuple2(this.first, this.second)
fun <T1, T2> tuple(v1: T1, v2: T2): Tuple2<T1, T2> = Tuple2(v1, v2)


inline fun <reified T : Any> measureTimeInMillis(functionToTime: () -> T): Pair<T, String> {
    val begin = System.currentTimeMillis()
    val ret = functionToTime()
    val end = System.currentTimeMillis()

    val millis = end - begin

    val second = millis / 1000 % 60
    val minute = millis / (1000 * 60) % 60
    val hour = millis / (1000 * 60 * 60) % 24

    val time = String.format("%02d:%02d:%02d:%d", hour, minute, second, millis)

    return Pair(ret, time)
}


fun getLocalSparkContext(appName: String, cores: Int = 4): JavaSparkContext {
    val sparkConf = SparkConf().setAppName(appName).setMaster("local[$cores]")
            //.set("spark.sql.shuffle.partitions", "1")
            .set("es.nodes", "localhost:9200")
            .set("es.nodes.discovery", "true")
            .set("es.nodes.wan.only", "false")
            .set("spark.default.parallelism", "8")
            .set("num-executors", "3")
            .set("executor-cores", "4")
            .set("executor-memory", "4G")

    val jsc = JavaSparkContext(sparkConf)
    return jsc
}

fun closeSpark(jsc: JavaSparkContext) {
    jsc.clearJobGroup()
    jsc.close()
    jsc.stop()
    println("exit spark job conf")
}

fun <K, V> JavaPairRDD<K, V>.combineByKeyIntoList(): JavaPairRDD<K, MutableList<V>> {

    val createListCombiner = Function<V, MutableList<V>> { x ->
        val res = mutableListOf<V>()
        res.add(x)
        res
    }

    val addCombiner = Function2<MutableList<V>, V, MutableList<V>> { list, x ->
        list.add(x)
        list
    }

    val combine = Function2<MutableList<V>, MutableList<V>, MutableList<V>> { list1, list2 ->
        list1.addAll(list2)
        list1
    }

    val combinedKeysWithLists = this.combineByKey(createListCombiner, addCombiner, combine)

    return combinedKeysWithLists

}

fun JavaPairRDD<String, Long>.reduceSplit(splitter : String = "@"): JavaPairRDD<String, Tuple2<String, Long>> {
    return this.reduceByKey { a, b -> a + b }.mapToPair({ tagsWithFreq ->
        val (tags, freq) = tagsWithFreq
        val (key, value) = tags.split(splitter)
        Tuple2(key, Tuple2(value, freq))
    })
}

fun deleteFileIfExist(fileName: String): Boolean {
    if (File(fileName).exists()) {
        deleteDirectory(File(fileName))
    }
    return !File(fileName).exists()
}

fun deleteDirectory(directory: File): Boolean {
    if (directory.exists()) {
        directory.listFiles().forEach { file ->
            if (file.isDirectory) {
                deleteDirectory(file)
            } else {
                file.delete()
            }
        }
    }
    return (directory.delete())
}




