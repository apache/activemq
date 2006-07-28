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
import org.apache.activemq.openwire.tool.OpenWireScript
import org.apache.activemq.openwire.tool.TestDataGenerator
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.FixCRLF;

/**
 * Generates the Java test code for the Open Wire Format
 *
 * @version $Revision: $
 */
class GenerateJavaTests extends OpenWireScript {

    Object run() {
    
        def openwireVersion = getProperty("version");
        
        def destDir = new File("src/test/java/org/apache/activemq/openwire/v${openwireVersion}")
        println "Generating Java test code to directory ${destDir}"
        
        def openwireClasses = classes.findAll {
        		it.getAnnotation("openwire:marshaller")!=null
        }

        def concreteClasses = new ArrayList()
        def buffer = new StringBuffer()
        int counter = 0
        Map map = [:]

        destDir.mkdirs()
        for (jclass in openwireClasses) {

            println "Processing ${jclass.simpleName}"
            def abstractText = "abstract "
            def classIsAbstract = isAbstract(jclass);
            def isBaseAbstract = isAbstract(jclass.superclass);
            if( !classIsAbstract ) {
              concreteClasses.add(jclass)
              abstractText = ""
            }
            
            def properties = jclass.declaredProperties.findAll { isValidProperty(it) }
            
            def testClassName = jclass.simpleName + "Test"
            if (classIsAbstract) 
                testClassName += "Support"

            def file = new File(destDir, testClassName + ".java")

            buffer << """
${jclass.simpleName}Test.class
"""

            file.withWriter { out |
out << """/**
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
package org.apache.activemq.openwire.v${openwireVersion};

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;
"""
for (pkg in jclass.importedPackages) {
    for (clazz in pkg.classes) {
       out << "import "+clazz.qualifiedName+";"
    }
}

def baseClass = "DataFileGeneratorTestSupport"
if (!jclass.superclass.simpleName.equals("JNDIBaseStorable") && !jclass.superclass.simpleName.equals("DataStructureSupport") && !jclass.superclass.simpleName.equals("Object") ) {
   baseClass = jclass.superclass.simpleName + "Test";
   if (isBaseAbstract) 
       baseClass += "Support"
}

def marshallerAware = isMarshallAware(jclass);
    
out << """

/**
 * Test case for the OpenWire marshalling for ${jclass.simpleName}
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version \$Revision: \$
 */
public ${abstractText}class $testClassName extends $baseClass {
"""
	if (!classIsAbstract) 
		out << """

    public static ${jclass.simpleName}Test SINGLETON = new ${jclass.simpleName}Test();

    public Object createObject() throws Exception {
    		${jclass.simpleName} info = new ${jclass.simpleName}();
    		populateObject(info);
    		return info;
    }
"""
	out << """
    
    protected void populateObject(Object object) throws Exception {
    		super.populateObject(object);
    		${jclass.simpleName} info = (${jclass.simpleName}) object;
"""

def generator = new TestDataGenerator();

for (property in properties) {
    def annotation = property.getter.getAnnotation("openwire:property");
    def size = annotation.getValue("size");
    def testSize = stringValue(annotation, "testSize");
    def type = property.type.simpleName
    def cached = isCachedProperty(property);
    def propertyName = property.simpleName;
    if (testSize == "-1") continue
    
    out << "        "
    switch (type) {
    case "boolean":
        out << "info.${property.setter.simpleName}(${generator.createBool()});"; break;
    case "byte":
        out << "info.${property.setter.simpleName}(${generator.createByte()});"; break;
    case "char":
        out << "info.${property.setter.simpleName}(${generator.createChar()});"; break;
    case "short":
        out << "info.${property.setter.simpleName}(${generator.createShort()});"; break;
    case "int":
        out << "info.${property.setter.simpleName}(${generator.createInt()});"; break;
    case "long":
	    out << "info.${property.setter.simpleName}(${generator.createLong()});"; break;
    case "byte[]":
	    out << """info.${property.setter.simpleName}(${generator.createByteArray(propertyName)});"""; break;
    case "String":
	    out << """info.${property.setter.simpleName}("${generator.createString(propertyName)}");"""; break;
    case "ByteSequence":
    		out << """
    		{
        		byte data[] = ${generator.createByteArray(propertyName)};
        		info.${property.setter.simpleName}(new org.apache.activeio.packet.ByteSequence(data,0,data.length));
    		}
    		""";
        break;
    case "Throwable":
	    out << """info.${property.setter.simpleName}(createThrowable("${generator.createString(propertyName)}"));"""; break;
    default:
    	    if( property.type.arrayType ) {
      	    def arrayType = property.type.arrayComponentType.simpleName;
      	    if (size == null) 
      	      size = 2
      	    if (arrayType == jclass.simpleName)
      	      size = 0
      	     	
      	    out << """
    		    {
	            ${arrayType} value[] = new ${arrayType}[${size}];
	            for( int i=0; i < ${size}; i++ ) {
	                value[i] = create${arrayType}("${generator.createString(propertyName)}");
	            }
	            info.${property.setter.simpleName}(value);
            }"""
        		} 
        		else {
        			out << """info.${property.setter.simpleName}(create${type}("${generator.createString(propertyName)}"));"""
            }
        }
        out.newLine() 
    }
        out << """
            }
        }
"""
}
        // Use the FixCRLF Ant Task to make sure the file has consistent newlines
        // so that SVN does not complain on checkin.
        Project project = new Project();
        project.init();
        FixCRLF fixCRLF = new FixCRLF();
        fixCRLF.setProject(project);
        fixCRLF.setSrcdir(file.getParentFile());
        fixCRLF.setIncludes(file.getName());
        fixCRLF.execute();

		}
    }
}