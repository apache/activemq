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

import java.util.ArrayList;

import org.apache.activemq.util.IntrospectionSupport;

/**
 * Helper utility that can be used to set the properties on any object
 * using command line arguments.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class CommandLineSupport {
	
	/**
	 * Sets the properties of an object given the command line args.
	 * 
	 * if args contains: --ack-mode=AUTO --url=tcp://localhost:61616 --persistent 
	 * 
	 * then it will try to call the following setters on the target object.
	 * 
	 * target.setAckMode("AUTO");
	 * target.setURL(new URI("tcp://localhost:61616") );
	 * target.setPersistent(true);
	 * 
	 * Notice the the proper conversion for the argument is determined by examining the 
	 * setter arguement type.  
	 * 
	 * @param target the object that will have it's properties set
	 * @param args the commline options
	 * @return any arguments that are not valid options for the target
	 */
	static public String[] setOptions(Object target, String []args) {
		ArrayList rc = new ArrayList();
		
		for (int i = 0; i < args.length; i++) {
			if( args[i] == null )
				continue;
			
			if( args[i].startsWith("--") ) {
				
				// --options without a specified value are considered boolean flags that are enabled.
				String value="true";
				String name = args[i].substring(2);
				
				// if --option=value case
				int p = name.indexOf("=");
				if( p > 0 ) {
					value = name.substring(p+1);
					name = name.substring(0,p);
				}
				
				// name not set, then it's an unrecognized option
				if( name.length()==0 ) {
					rc.add(args[i]);
					continue;
				}
				
				String propName = convertOptionToPropertyName(name);
				if( !IntrospectionSupport.setProperty(target, propName, value) ) {					
					rc.add(args[i]);
					continue;
				}
			}
			
		}
		
		String r[] = new String[rc.size()];
		rc.toArray(r);
		return r;
	}

	/**
	 * converts strings like: test-enabled to testEnabled
	 * @param name
	 * @return
	 */
	private static String convertOptionToPropertyName(String name) {
		String rc="";
		
		// Look for '-' and strip and then convert the subsequent char to uppercase
		int p = name.indexOf("-");
		while( p > 0 ) {
			// strip
			rc += name.substring(0, p);
			name = name.substring(p+1);
			
			// can I convert the next char to upper?
			if( name.length() >0 ) {
				rc += name.substring(0,1).toUpperCase();
				name = name.substring(1);
			}
			
			p = name.indexOf("-");
		}
		return rc+name;
	}
}
