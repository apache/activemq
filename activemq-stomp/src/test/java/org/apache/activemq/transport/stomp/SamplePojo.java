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
package org.apache.activemq.transport.stomp;

import java.io.Serializable;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("pojo")
public class SamplePojo implements Serializable {
    private static final long serialVersionUID = 9118938642100015088L;

    @XStreamAlias("name")
    private String name;
    @XStreamAlias("city")
    private String city;

    public SamplePojo() {
    }

    public SamplePojo(String name, String city) {
        this.name = name;
        this.city = city;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //implement equals

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SamplePojo) {
            SamplePojo other = (SamplePojo) obj;
            return name.equals(other.name) && city.equals(other.city);
        }
        return false;
    }

}
