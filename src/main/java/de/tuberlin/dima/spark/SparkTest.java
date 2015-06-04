package de.tuberlin.dima.spark;

import de.tuberlin.dima.flink.model.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;

/**
 * Created by mustafa on 03/06/15.
 */
public class SparkTest {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("MutaubleDataSetDemoSpark").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> personJavaRDD = sc.textFile("../MutuableDatasetDemo/src/main/resources/Person.csv");


	}
}
