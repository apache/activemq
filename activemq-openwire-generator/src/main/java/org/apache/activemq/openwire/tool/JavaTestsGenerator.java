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
package org.apache.activemq.openwire.tool;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JPackage;
import org.codehaus.jam.JProperty;


/**
 * 
 * @version $Revision: 384826 $
 */
public class JavaTestsGenerator extends MultiSourceGenerator {

	protected String targetDir="src/test/java";

	public Object run() {
		if (destDir == null) {
			destDir = new File(targetDir+"/org/apache/activemq/openwire/v" + getOpenwireVersion());
		}
		return super.run();
	}
	
	protected String getClassName(JClass jclass) {
    	if( isAbstract(jclass) ) {
        	return super.getClassName(jclass) + "TestSupport";
    	} else {
        	return super.getClassName(jclass) + "Test";
    	}
	}

    protected String getBaseClassName(JClass jclass) {
        String answer = "DataFileGeneratorTestSupport";
        if (superclass != null) {
            String name = superclass.getSimpleName();
            if (name!=null 
            		&& !name.equals("JNDIBaseStorable") 
    				&& !name.equals("DataStructureSupport") 
    				&& !name.equals("Object")) {
        	   answer = name + "Test";
    		   if (isAbstract(getJclass().getSuperclass())) 
    			   answer += "Support";
            }
        }
        return answer;
    }

	private void generateLicence(PrintWriter out) {
out.println("/**");
out.println(" *");
out.println(" * Licensed to the Apache Software Foundation (ASF) under one or more");
out.println(" * contributor license agreements.  See the NOTICE file distributed with");
out.println(" * this work for additional information regarding copyright ownership.");
out.println(" * The ASF licenses this file to You under the Apache License, Version 2.0");
out.println(" * (the \"License\"); you may not use this file except in compliance with");
out.println(" * the License.  You may obtain a copy of the License at");
out.println(" *");
out.println(" * http://www.apache.org/licenses/LICENSE-2.0");
out.println(" *");
out.println(" * Unless required by applicable law or agreed to in writing, software");
out.println(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
out.println(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
out.println(" * See the License for the specific language governing permissions and");
out.println(" * limitations under the License.");
out.println(" */");
	}
	
	protected void generateFile(PrintWriter out) {

		generateLicence(out);

out.println("package org.apache.activemq.openwire.v"+openwireVersion+";");
out.println("");
out.println("import java.io.DataInputStream;");
out.println("import java.io.DataOutputStream;");
out.println("import java.io.IOException;");
out.println("");
out.println("import org.apache.activemq.openwire.*;");
out.println("import org.apache.activemq.command.*;");
out.println("");
		for (int i = 0; i < getJclass().getImportedPackages().length; i++) {
			JPackage pkg = getJclass().getImportedPackages()[i];
			for (int j = 0; j < pkg.getClasses().length; j++) {
				JClass clazz = pkg.getClasses()[j];
out.println("import " + clazz.getQualifiedName() + ";");
			}
		}

out.println("");
out.println("/**");
out.println(" * Test case for the OpenWire marshalling for "+jclass.getSimpleName()+"");
out.println(" *");
out.println(" *");
out.println(" * NOTE!: This file is auto generated - do not modify!");
out.println(" *        if you need to make a change, please see the modify the groovy scripts in the");
out.println(" *        under src/gram/script and then use maven openwire:generate to regenerate ");
out.println(" *        this file.");
out.println(" *");
out.println(" * @version $Revision: $");
out.println(" */");
out.println("public "+getAbstractClassText()+"class "+className+" extends "+baseClass+" {");
out.println("");
		if (!isAbstractClass()) {
out.println("");
out.println("    public static "+jclass.getSimpleName()+"Test SINGLETON = new "+jclass.getSimpleName()+"Test();");
out.println("");
out.println("    public Object createObject() throws Exception {");
out.println("        "+jclass.getSimpleName()+" info = new "+jclass.getSimpleName()+"();");
out.println("        populateObject(info);");
out.println("        return info;");
out.println("    }");
		}
out.println("");
out.println("    protected void populateObject(Object object) throws Exception {");
out.println("        super.populateObject(object);");
out.println("        "+getJclass().getSimpleName()+" info = ("+getJclass().getSimpleName()+") object;");
out.println("");

		TestDataGenerator generator = new TestDataGenerator();

		List properties = getProperties();
		for (Iterator iter = properties.iterator(); iter.hasNext();) {
			JProperty property = (JProperty) iter.next();

			JAnnotation annotation = property.getAnnotation("openwire:property");		   
			String size = stringValue(annotation, "size");
			String testSize = stringValue(annotation, "testSize");
		    String type = property.getType().getSimpleName();
		    boolean cached = isCachedProperty(property);
		    String propertyName = property.getSimpleName();
		    if ("-1".equals(testSize)) 
		    	continue;
		   		    
		    
		    String setterName = property.getSetter().getSimpleName();
		    
		    if( type.equals("boolean")) {
out.println("        info."+setterName+"("+generator.createBool()+");");
		    } else if( type.equals("byte")) {
out.println("        info."+setterName+"("+generator.createByte()+");");
		    } else if( type.equals("char")) {
out.println("        info."+setterName+"("+generator.createChar()+");");
		    } else if( type.equals("short")) {
out.println("        info."+setterName+"("+generator.createShort()+");");
		    } else if( type.equals("int")) {
out.println("        info."+setterName+"("+generator.createInt()+");");
		    } else if( type.equals("long")) {
out.println("        info."+setterName+"("+generator.createLong()+");");
		    } else if( type.equals("byte[]")) {
out.println("        info."+setterName+"("+generator.createByteArray(propertyName)+");");
		    } else if( type.equals("String")) {
out.println("        info."+setterName+"(\""+generator.createString(propertyName)+"\");");
		    } else if( type.equals("ByteSequence")) {
out.println("        {");
out.println("            byte data[] = "+generator.createByteArray(propertyName)+";");
out.println("            info."+setterName+"(new org.apache.activemq.util.ByteSequence(data,0,data.length));");
out.println(        "}");       
		    } else if( type.equals("Throwable")) {
out.println("        info."+setterName+"(createThrowable(\""+generator.createString(propertyName)+"\"));");
		    } else {
			    if( property.getType().isArrayType() ) {
			    	String arrayType = property.getType().getArrayComponentType().getSimpleName();
		      	    if (size == null) 
		      	      size = "2";
			  	    if (arrayType == jclass.getSimpleName())
			  	      size = "0";
out.println("        {");
out.println("            "+arrayType+" value[] = new "+arrayType+"["+size+"];");
out.println("            for( int i=0; i < "+size+"; i++ ) {");
out.println("                value[i] = create"+arrayType+"(\""+generator.createString(propertyName)+"\");");
out.println("            }");
out.println("            info."+setterName+"(value);");
out.println("        }");
	    		} else {
out.println("        info."+setterName+"(create"+type+"(\""+generator.createString(propertyName)+"\"));");
	            }
	        }
	    }
            
out.println("    }"); 
out.println("}");  
	}

	public String getTargetDir() {
		return targetDir;
	}

	public void setTargetDir(String targetDir) {
		this.targetDir = targetDir;
	}
}

