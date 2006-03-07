/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
import org.apache.activemq.openwire.tool.OpenWireJavaMarshallingScript

/**
 * Generates the Java marshalling code for the Open Wire Format
 *
 * @version $Revision$
 */
class GenerateJavaMarshalling extends OpenWireJavaMarshallingScript {

 	void generateFile(PrintWriter out) {
        out << """/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
out << """

/**
 * Marshalling code for Open Wire Format for ${className}
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version \$Revision\$
 */
public ${abstractClassText}class ${className} extends ${baseClass} {
"""

if( !abstractClass ) out << """
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

out << """
    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {
        super.tightUnmarshal(wireFormat, o, dataIn, bs);
"""

if( !properties.isEmpty() )  out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""

if( marshallerAware ) out << """
        info.beforeUnmarshall(wireFormat);
        
"""

generateTightUnmarshalBody(out)

if( marshallerAware ) out << """
        info.afterUnmarshall(wireFormat);
"""

out << """
    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
"""


if( !properties.isEmpty() ) out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""


if( marshallerAware ) out << """
        info.beforeMarshall(wireFormat);
"""

out << """
        int rc = super.tightMarshal1(wireFormat, o, bs);
"""

def baseSize = generateTightMarshal1Body(out)
    
out << """
        return rc + ${baseSize};
    }

    /**
     * Write a object instance to data output stream
     *
     * @param o the instance to be marshaled
     * @param dataOut the output stream
     * @throws IOException thrown if an error occurs
     */
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        super.tightMarshal2(wireFormat, o, dataOut, bs);
"""

if( !properties.isEmpty() ) out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""

generateTightMarshal2Body(out)

if( marshallerAware ) out << """
        info.afterMarshall(wireFormat);
"""

out << """
    }
"""

out << """
    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn) throws IOException {
        super.looseUnmarshal(wireFormat, o, dataIn);
"""

if( !properties.isEmpty() )  out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""

if( marshallerAware ) out << """
        info.beforeUnmarshall(wireFormat);
        
"""

generateLooseUnmarshalBody(out)

if( marshallerAware ) out << """
        info.afterUnmarshall(wireFormat);
"""

out << """
    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut) throws IOException {
"""

if( !properties.isEmpty() ) out << """
        ${jclass.simpleName} info = (${jclass.simpleName})o;
"""


if( marshallerAware ) out << """
        info.beforeMarshall(wireFormat);
"""

out << """
        super.looseMarshal(wireFormat, o, dataOut);
"""

generateLooseMarshalBody(out)
    
out << """
    }
}
"""
        }
 	
 
    	void generateFactory(PrintWriter out) {
            out << """/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */

package org.apache.activemq.openwire.v${openwireVersion};

import org.apache.activemq.openwire.DataStreamMarshaller;
import org.apache.activemq.openwire.OpenWireFormat;

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