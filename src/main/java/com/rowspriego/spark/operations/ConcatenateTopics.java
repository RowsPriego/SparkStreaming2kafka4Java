package com.rowspriego.spark.operations;

import org.apache.spark.api.java.function.Function2;

public class ConcatenateTopics implements Function2<String, String, String> {

	@Override
	public String call(String v1, String v2) throws Exception {
		return v1 + "/n" + v2;
	}

}
