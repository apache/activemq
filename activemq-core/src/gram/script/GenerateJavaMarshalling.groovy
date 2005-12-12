/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
import org.activemq.openwire.tool.OpenWireScript

/**
 * Generates the Java marshalling code for the Open Wire Format
 *
 * @version $Revision$
 */
class GenerateJavaMarshalling extends OpenWireScript {

    Object run() {
    
        def openwireVersion = System.getProperty("openwire.version");
        
        def destDir = new File("src/main/java/org/activemq/openwire/v${openwireVersion}")
        println "Generating Java marshalling code to directory ${destDir}"
        
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
            def isAbstract = isAbstract(jclass);
            if( !isAbstract ) {
              concreteClasses.add(jclass)
            }
            
            def properties = jclass.declaredProperties.findAll { isValidProperty(it) }
            
            def file = new File(destDir, jclass.simpleName + "Marshaller.java")

            buffer << """
${jclass.simpleName}Marshaller.class
"""

            file.withWriter { out |
out << """/** 
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a> 
 * 
 * Copyright 2005 Hiram Chirino
 * Copyright 2005 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activemq.openwire.v${openwireVersion};

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.activemq.openwire.*;
import org.activemq.command.*;
"""
for (pkg in jclass.importedPackages) {
    for (clazz in pkg.classes) {
       out << "import "+clazz.qualifiedName+";"
    }
}

def baseClass = "org.activemq.openwire.DataStreamMarshaller"
if( !jclass.superclass.simpleName.equals("Object") ) {
   baseClass = jclass.superclass.simpleName + "Marshaller";
}

def marshallerAware = isMarshallAware(jclass);

out << """

/**
 * Marshalling code for Open Wire Format for ${jclass.simpleName}
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version \$Revision\$
 */
public """+ (isAbstract?"abstract ":"") + """class ${jclass.simpleName}Marshaller extends ${baseClass} {
"""
if( !isAbstract ) {
out << """
    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return ${jclass.simpleName}.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new ${jclass.simpleName}();
    }
"""
}
out << """
    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void unmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {
        super.unmarshal(wireFormat, o, dataIn, bs);
"""
if( !properties.isEmpty() ) {
out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""
}
if( marshallerAware ) {
out << """
        info.beforeUnmarshall(wireFormat);
        
"""
}
for (property in properties) {
    def annotation = property.getter.getAnnotation("openwire:property");
    def size = annotation.getValue("size");
    def type = property.type.qualifiedName
    def cached = isCachedProperty(property);
    
    out << "        "
    switch (type) {
    case "boolean":
        out << "info.${property.setter.simpleName}(bs.readBoolean());"; break;
    case "byte":
        out << "info.${property.setter.simpleName}(dataIn.readByte());"; break;
    case "char":
        out << "info.${property.setter.simpleName}(dataIn.readChar());"; break;
    case "short":
        out << "info.${property.setter.simpleName}(dataIn.readShort());"; break;
    case "int":
        out << "info.${property.setter.simpleName}(dataIn.readInt());"; break;
    case "long":
	    out << "info.${property.setter.simpleName}(unmarshalLong(wireFormat, dataIn, bs));"; break;
    case "byte[]":
        if( size != null ) {
        		out << """{
        		byte data[] = new byte[${size.asInt()}];
        		dataIn.readFully(data);
        		info.${property.setter.simpleName}(data);
    		}
    		""";
        } else {
        		out << """{
        		if( bs.readBoolean() ) {
	        		int size = dataIn.readInt();
	        		byte data[] = new byte[size];
	        		dataIn.readFully(data);
	        		info.${property.setter.simpleName}(data);
        		} else {
	        		info.${property.setter.simpleName}(null);
        		}
        }
    		""";
        }
        break;
    case "org.activeio.ByteSequence":
    		out << """
    		if( bs.readBoolean() ) {
        		int size = dataIn.readInt();
        		byte data[] = new byte[size];
        		dataIn.readFully(data);
        		info.${property.setter.simpleName}(new org.activeio.ByteSequence(data,0,size));
    		} else {
        		info.${property.setter.simpleName}(null);
    		}
    		""";
        break;
    case "java.lang.String":
        out << "info.${property.setter.simpleName}(readString(dataIn, bs));"; break;
    default:
    	    if( property.type.arrayType ) {
      	    def arrayType = property.type.arrayComponentType.qualifiedName;
    	    		if( size!=null ) { 
        			out << """
	            ${arrayType} value[] = new ${arrayType}[${size}];
	            for( int i=0; i < ${size}; i++ ) {
	                value[i] = (${arrayType})unmarsalNestedObject(wireFormat,dataIn, bs);
	            }
	            info.${property.setter.simpleName}(value);
        			"""
        		} else {
        			out << """
		        if( bs.readBoolean() ) {
		            short size = dataIn.readShort();
		            ${arrayType} value[] = new ${arrayType}[size];
		            for( int i=0; i < size; i++ ) {
		                value[i] = (${arrayType})unmarsalNestedObject(wireFormat,dataIn, bs);
		            }
		            info.${property.setter.simpleName}(value);
		        } else {
		            info.${property.setter.simpleName}(null);
		        }
        			"""
        		}
    	    } else if( isThrowable(property.type) ) {
	        out << "info.${property.setter.simpleName}((${type}) unmarsalThrowable(wireFormat, dataIn, bs));"
        } else {
            if ( cached ) {
	        	   out << "info.${property.setter.simpleName}((${type}) unmarsalCachedObject(wireFormat, dataIn, bs));"
	        } else {
	        	   out << "info.${property.setter.simpleName}((${type}) unmarsalNestedObject(wireFormat, dataIn, bs));"
	        }
        }
    }
    out << """
"""
           }
if( marshallerAware ) {
out << """
        info.afterUnmarshall(wireFormat);
"""
}
    out << """
    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
"""
if( !properties.isEmpty() ) {
out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""
}
if( marshallerAware ) {
out << """
        info.beforeMarshall(wireFormat);
"""
}
	def baseSize=0;
out << """
        int rc = super.marshal1(wireFormat, o, bs);
"""
for (property in properties) {
    def annotation = property.getter.getAnnotation("openwire:property");
    def size = annotation.getValue("size");
    def getter = "info." + property.getter.simpleName + "()"
    def cached = isCachedProperty(property);
    
    out << "        "

    def type = property.type.qualifiedName
    switch (type) {
    case "boolean":
        out << "bs.writeBoolean($getter);"; break;
    case "byte": baseSize+=1; break;
    case "char": baseSize+=2; break;
    case "short": baseSize+=2; break;
    case "int": baseSize+=4; break;
    case "long":         
        out << "rc+=marshal1Long(wireFormat, $getter, bs);"; break;
    case "byte[]":
        if( size ==null ) {
		out << """
		bs.writeBoolean($getter!=null);
		rc += ${getter}==null ? 0 : ${getter}.length+4;
		""";
		} else {
			baseSize += size.asInt();
		}
        break;
    case "org.activeio.ByteSequence":
		out << """
		bs.writeBoolean($getter!=null);
		rc += ${getter}==null ? 0 : ${getter}.getLength()+4;
		""";
        break;
    case "java.lang.String":
        out << "rc += writeString($getter, bs);"; break;
    default:
    	    if( property.type.arrayType ) {
    	    		if( size!=null ) { 
        			out << "rc += marshalObjectArrayConstSize(wireFormat, $getter, bs, $size);"; break;
        		} else {
        			out << "rc += marshalObjectArray(wireFormat, $getter, bs);"; break;
        		}
    	    } else if( isThrowable(property.type) ) {    	    
        		out << "rc += marshalThrowable(wireFormat, $getter, bs);"; break;
    	    } else {
    	    		if( cached ) { 
        			out << "rc += marshal1CachedObject(wireFormat, $getter, bs);"; break;
        		} else {
        			out << "rc += marshal1NestedObject(wireFormat, $getter, bs);"; break;
        		}
        }
    }
    out << """
"""
        }
    out << """
        return rc+${baseSize};
    }

    /**
     * Write a object instance to data output stream
     *
     * @param o the instance to be marshaled
     * @param dataOut the output stream
     * @throws IOException thrown if an error occurs
     */
    public void marshal2(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        super.marshal2(wireFormat, o, dataOut, bs);
"""
if( !properties.isEmpty() ) {
out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""
}
for (property in properties) {
    def annotation = property.getter.getAnnotation("openwire:property");
    def size = annotation.getValue("size");
    def getter = "info." + property.getter.simpleName + "()"
    def cached = isCachedProperty(property);
    
    out << "        "

    def type = property.type.qualifiedName
    switch (type) {
    case "boolean":
        out << "bs.readBoolean();"; break;
    case "byte":
        out << "dataOut.writeByte($getter);"; break;
    case "char":
        out << "dataOut.writeChar($getter);"; break;
    case "short":
        out << "dataOut.writeShort($getter);"; break;
    case "int":
        out << "dataOut.writeInt($getter);"; break;
    case "long":
        out << "marshal2Long(wireFormat, $getter, dataOut, bs);"; break;
    case "byte[]":
        if( size !=null ) {
        		out << "dataOut.write($getter, 0, ${size.asInt()});";
		} else {
		out << """
		if(bs.readBoolean()) { 
			dataOut.writeInt(${getter}.length);
			dataOut.write(${getter});
		}
		""";
        }
        break;
    case "org.activeio.ByteSequence":
		out << """
		if(bs.readBoolean()) { 
             org.activeio.ByteSequence data = ${getter};
			dataOut.writeInt(data.getLength());
			dataOut.write(data.getData(), data.getOffset(), data.getLength());
		}
		""";
        break;
    case "java.lang.String":
        out << "writeString($getter, dataOut, bs);"; break;
    default:
    	    if( property.type.arrayType ) {    	    
    	    		if( size!=null ) { 
        			out << "marshalObjectArrayConstSize(wireFormat, $getter, dataOut, bs, $size);"; break;
        		} else {
        			out << "marshalObjectArray(wireFormat, $getter, dataOut, bs);"; break;
        		}    	    
    	    } else if( isThrowable(property.type) ) {    	    
        		out << "marshalThrowable(wireFormat, $getter, dataOut, bs);"; break;
    	    } else {
    	    		if( cached ) {
	        		out << "marshal2CachedObject(wireFormat, $getter, dataOut, bs);"; break;
    	    		} else {
	        		out << "marshal2NestedObject(wireFormat, $getter, dataOut, bs);"; break;
    	    		}
        }
    }
    out << """
"""
        }
if( marshallerAware ) {
out << """
        info.afterMarshall(wireFormat);
"""
}
    out << """
    }
}
"""
            }
        }

        def file = new File(destDir, "MarshallerFactory.java")
        file.withWriter { out |
out << """/** 
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a> 
 * 
 * Copyright 2005 Hiram Chirino
 * Copyright 2005 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activemq.openwire.v${openwireVersion};

import org.activemq.openwire.DataStreamMarshaller;
import org.activemq.openwire.OpenWireFormat;

/**
 * MarshallerFactory for Open Wire Format.
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version \$Revision\$
 */
public class MarshallerFactory {

    /**
     * Creates a Map of command type -> Marshallers
     */
    static final private DataStreamMarshaller marshaller[] = new DataStreamMarshaller[256];
    static {
"""
for (jclass in concreteClasses) {
out << """
        add(new ${jclass.simpleName}Marshaller());"""
}        
out << """

	}

	static private void add(DataStreamMarshaller dsm) {
        marshaller[dsm.getDataStructureType()] = dsm;
    }
	
    static public DataStreamMarshaller[] createMarshallerMap(OpenWireFormat wireFormat) {
        return marshaller;
    }
}
"""
        }

    }
}