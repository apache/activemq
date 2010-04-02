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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.activemq.store.jdbc.Statements;


public class GenerateJDBCStatements {
    public static String returnStatement(Object statement){
    	return ((String)statement).replace("<", "&lt;").replace(">", "&gt;");
    	
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
    	Statements s=new Statements();
    	s.setTablePrefix("ACTIVEMQ.");
    	String[] stats=s.getCreateSchemaStatements();
    	System.out.println("<bean id=\"statements\" class=\"org.apache.activemq.store.jdbc.Statements\">");
    	System.out.println("<property name=\"createSchemaStatements\">");
    	System.out.println("<list>");
    	for(int i=0; i<stats.length;i++){
    		System.out.println("<value>"+stats[i]+"</value>");
    	}
    	System.out.println("</list>");
    	System.out.println("</property>");
    	
    	
    	Method[] methods=Statements.class.getMethods();
    	Pattern sPattern= Pattern.compile("get.*Statement$");
    	Pattern setPattern= Pattern.compile("set.*Statement$");
    	ArrayList<String> setMethods=new ArrayList<String>();
    	for(int i=0; i<methods.length;i++){
    		if(setPattern.matcher(methods[i].getName()).find()){
    			setMethods.add(methods[i].getName());
    		}
    	}
    	for(int i=0; i<methods.length;i++){
    		if(sPattern.matcher(methods[i].getName()).find()&&setMethods.contains(methods[i].getName().replace("get","set"))){
    			System.out.println("<property name=\""+methods[i].getName().substring(3,4).toLowerCase()+methods[i].getName().substring(4)+"\" value=\""+returnStatement(methods[i].invoke(s, null))+"\" />");
    		}
    	}
    	//for a typo is not needed if removeMessageStatment typo is corrected
    	Pattern sPattern2= Pattern.compile("get.*Statment$");
    	for(int i=0; i<methods.length;i++){
    		if(sPattern2.matcher(methods[i].getName()).find()){
    			System.out.println("<property name=\""+methods[i].getName().substring(3,4).toLowerCase()+methods[i].getName().substring(4)+"\" value=\""+returnStatement(methods[i].invoke(s, null))+"\" />");
    		}
    	}
    	//end of generating because of typo
    	
    	String[] statsDrop=s.getDropSchemaStatements();
    	System.out.println("<property name=\"dropSchemaStatements\">");
    	System.out.println("<list>");
    	for(int i=0; i<statsDrop.length;i++){
    		System.out.println("<value>"+statsDrop[i]+"</value>");
    	}
    	System.out.println("</list>");
    	System.out.println("</property>");
    	System.out.println("</bean>");
    	

	}

}
