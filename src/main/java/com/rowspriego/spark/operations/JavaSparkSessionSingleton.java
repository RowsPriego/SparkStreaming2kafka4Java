package com.rowspriego.spark.operations;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSessionSingleton {

	private transient static SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		return instance;
	}
}
