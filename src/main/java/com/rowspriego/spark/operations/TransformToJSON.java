package com.rowspriego.spark.operations;

import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.rowspriego.spark.model.Event;

import scala.Tuple2;

public class TransformToJSON implements Function<Tuple2<String, String>, Event> {

	@Override
	public Event call(Tuple2<String, String> arg0) throws Exception {
		Event event = new Event();
		JSONParser parser = new JSONParser();
		JSONObject json = null;

		if (arg0 != null) {
			try {
				json = (JSONObject) parser.parse(arg0._2());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// Modificaciones en el JSON para adaptarse a mi JSON de
			// salida

			if (json.containsKey("eid")) {
				event.setEid((String) json.get("eid"));
			}

			if (json.containsKey("passport")) {
				JSONObject passport = (JSONObject) json.get("passport");
				if (passport.containsKey("sid")) {
					event.setSessionId((String) passport.get("sid"));
				} else {
					event.setSessionId("");
				}
			} else {
				event.setSessionId("");
			}

			if (json.containsKey("userId")) {
				event.setUserId((String) json.get("userId"));
			}

			return event;
		}

		return null;
	}

}
