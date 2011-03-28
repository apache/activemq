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
package org.apache.activemq.util;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;

public class SimplePojo {

    String name;
    int age;
    boolean enabled;
    URI uri;
    List<ActiveMQDestination> favorites = new ArrayList<ActiveMQDestination>();
    List<ActiveMQDestination> nonFavorites = new ArrayList<ActiveMQDestination>();
    List<ActiveMQDestination> others = new ArrayList<ActiveMQDestination>();
    
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public boolean isEnabled() {
        return enabled;
    }
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public URI getUri() {
        return uri;
    }
    public void setUri(URI uri) {
        this.uri = uri;
    }
	public List<ActiveMQDestination> getFavorites() {
		return favorites;
	}
	public void setFavorites(List<ActiveMQDestination> favorites) {
		this.favorites = favorites;
	}
	public List<ActiveMQDestination> getNonFavorites() {
		return nonFavorites;
	}
	public void setNonFavorites(List<ActiveMQDestination> nonFavorites) {
		this.nonFavorites = nonFavorites;
	}
	public List<ActiveMQDestination> getOthers() {
		return others;
	}
	public void setOthers(List<ActiveMQDestination> others) {
		this.others = others;
	}
    
}
