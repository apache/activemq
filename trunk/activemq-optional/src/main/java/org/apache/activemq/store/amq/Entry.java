/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.amq;

import org.apache.activemq.command.DataStructure;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.util.ByteSequence;
import org.josql.Query;

public class Entry {
	
	Location location;
	DataStructure record;
	private ByteSequence data;
	private String type;
	private String formater;
	private Query query;
	
	public Location getLocation() {
		return location;
	}
	public void setLocation(Location location) {
		this.location = location;
	}
	public DataStructure getRecord() {
		return record;
	}
	public void setRecord(DataStructure record) {
		this.record = record;
	}
	public void setData(ByteSequence data) {
		this.data = data;
	}
	public void setType(String type) {
		this.type = type;
	}
	public ByteSequence getData() {
		return data;
	}
	public String getType() {
		return type;
	}
	public void setFormater(String formater) {
		this.formater = formater;
	}
	public String getFormater() {
		return formater;
	}
	public void setQuery(Query query) {
		this.query = query;
	}
	public Query getQuery() {
		return query;
	}
	
}
