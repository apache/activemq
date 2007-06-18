/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Generic utility class that can be used by the examples 
 */
public class Utilities {
	
	/**
	 * Converts an array of objects to a string 
	 * Useful only for Java 1.4 and below, because of the unavailability of the Arrays.toString() method.
	 */
	static public String arrayToString(Object target[]) {
	    StringBuffer buf = new StringBuffer("{");
	    int i;
	    for (i=0; i<target.length-1; i++) {
	        buf.append(target[i].toString());
	        buf.append(", ");
	    }
	    buf.append(target[i].toString());
	    buf.append("}");
	    return buf.toString();
	}
}
