package com.rowspriego.spark.model;

import java.io.Serializable;

public class Event implements Serializable {

	private String eid;
	private String sessionId;
	private String timestamp;
	private String name;
	private String email;
	private String type;
	private String userId;

	public String getEid() {
		return eid;
	}

	public void setEid(String eid) {
		this.eid = eid;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String toString() {
		return "{\"eid\":\"" + eid + "\",\"sessionId\":\"" + sessionId + "\",\"name\":\"" + name + "\",\"type\":\""
				+ type + "\",\"userId\":\"" + userId + "\"}";

	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}
}
