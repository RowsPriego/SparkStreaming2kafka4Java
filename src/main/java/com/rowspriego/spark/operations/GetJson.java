package com.rowspriego.spark.operations;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class GetJson implements Function<Tuple2<String, String>, String> {

	@Override
	public String call(Tuple2<String, String> v1) throws Exception {
		return v1._2;
	}

}
