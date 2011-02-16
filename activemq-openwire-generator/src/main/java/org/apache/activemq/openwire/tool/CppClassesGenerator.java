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

import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

/**
 * 
 */
public class CppClassesGenerator extends MultiSourceGenerator {

    protected String targetDir = "./src/main/cpp";

    public Object run() {
        filePostFix = getFilePostFix();
        if (destDir == null) {
            destDir = new File(targetDir + "/activemq/command");
        }
        return super.run();
    }

    protected String getFilePostFix() {
        return ".cpp";
    }

    /**
     * Converts the Java type to a C++ type name
     */
    public String toCppType(JClass type) {
        String name = type.getSimpleName();
        if (name.equals("String")) {
            return "p<string>";
        } else if (type.isArrayType()) {
            if (name.equals("byte[]")) {
                name = "char[]";
            } else if (name.equals("DataStructure[]")) {
                name = "IDataStructure[]";
            }
            return "array<" + name.substring(0, name.length() - 2) + ">";
        } else if (name.equals("Throwable") || name.equals("Exception")) {
            return "p<BrokerError>";
        } else if (name.equals("ByteSequence")) {
            return "array<char>";
        } else if (name.equals("boolean")) {
            return "bool";
        } else if (name.equals("long")) {
            return "long long";
        } else if (name.equals("byte")) {
            return "char";
        } else if (name.equals("Command") || name.equals("DataStructure")) {
            return "p<I" + name + ">";
        } else if (!type.isPrimitiveType()) {
            return "p<" + name + ">";
        } else {
            return name;
        }
    }

    /**
     * Converts the Java type to a C++ default value
     */
    public String toCppDefaultValue(JClass type) {
        String name = type.getSimpleName();

        if (name.equals("boolean")) {
            return "false";
        } else if (!type.isPrimitiveType()) {
            return "NULL";
        } else {
            return "0";
        }
    }

    /**
     * Converts the Java type to the name of the C++ marshal method to be used
     */
    public String toMarshalMethodName(JClass type) {
        String name = type.getSimpleName();
        if (name.equals("String")) {
            return "marshalString";
        } else if (type.isArrayType()) {
            if (type.getArrayComponentType().isPrimitiveType() && name.equals("byte[]")) {
                return "marshalByteArray";
            } else {
                return "marshalObjectArray";
            }
        } else if (name.equals("ByteSequence")) {
            return "marshalByteArray";
        } else if (name.equals("short")) {
            return "marshalShort";
        } else if (name.equals("int")) {
            return "marshalInt";
        } else if (name.equals("long")) {
            return "marshalLong";
        } else if (name.equals("byte")) {
            return "marshalByte";
        } else if (name.equals("double")) {
            return "marshalDouble";
        } else if (name.equals("float")) {
            return "marshalFloat";
        } else if (name.equals("boolean")) {
            return "marshalBoolean";
        } else if (!type.isPrimitiveType()) {
            return "marshalObject";
        } else {
            return name;
        }
    }

    /**
     * Converts the Java type to the name of the C++ unmarshal method to be used
     */
    public String toUnmarshalMethodName(JClass type) {
        String name = type.getSimpleName();
        if (name.equals("String")) {
            return "unmarshalString";
        } else if (type.isArrayType()) {
            if (type.getArrayComponentType().isPrimitiveType() && name.equals("byte[]")) {
                return "unmarshalByteArray";
            } else {
                return "unmarshalObjectArray";
            }
        } else if (name.equals("ByteSequence")) {
            return "unmarshalByteArray";
        } else if (name.equals("short")) {
            return "unmarshalShort";
        } else if (name.equals("int")) {
            return "unmarshalInt";
        } else if (name.equals("long")) {
            return "unmarshalLong";
        } else if (name.equals("byte")) {
            return "unmarshalByte";
        } else if (name.equals("double")) {
            return "unmarshalDouble";
        } else if (name.equals("float")) {
            return "unmarshalFloat";
        } else if (name.equals("boolean")) {
            return "unmarshalBoolean";
        } else if (!type.isPrimitiveType()) {
            return "unmarshalObject";
        } else {
            return name;
        }
    }

    /**
     * Converts the Java type to a C++ pointer cast
     */
    public String toUnmarshalCast(JClass type) {
        String name = toCppType(type);

        if (name.startsWith("p<")) {
            return "p_cast<" + name.substring(2);
        } else if (name.startsWith("array<") && (type.isArrayType() && !type.getArrayComponentType().isPrimitiveType()) && !type.getSimpleName().equals("ByteSequence")) {
            return "array_cast<" + name.substring(6);
        } else {
            return "";
        }
    }

    protected void generateLicence(PrintWriter out) {
        out.println("/**");
        out.println(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        out.println(" * contributor license agreements.  See the NOTICE file distributed with");
        out.println(" * this work for additional information regarding copyright ownership.");
        out.println(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        out.println(" * (the \"License\"); you may not use this file except in compliance with");
        out.println(" * the License.  You may obtain a copy of the License at");
        out.println(" *");
        out.println(" *      http://www.apache.org/licenses/LICENSE-2.0");
        out.println(" *");
        out.println(" * Unless required by applicable law or agreed to in writing, software");
        out.println(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        out.println(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        out.println(" * See the License for the specific language governing permissions and");
        out.println(" * limitations under the License.");
        out.println(" */");
    }

    protected void generateFile(PrintWriter out) throws Exception {
        generateLicence(out);
        out.println("#include \"activemq/command/" + className + ".hpp\"");
        out.println("");
        out.println("using namespace apache::activemq::command;");
        out.println("");
        out.println("/*");
        out.println(" *");
        out.println(" *  Command and marshalling code for OpenWire format for " + className + "");
        out.println(" *");
        out.println(" *");
        out.println(" *  NOTE!: This file is autogenerated - do not modify!");
        out.println(" *         if you need to make a change, please see the Groovy scripts in the");
        out.println(" *         activemq-core module");
        out.println(" *");
        out.println(" */");
        out.println("" + className + "::" + className + "()");
        out.println("{");

        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            String value = toCppDefaultValue(property.getType());
            String propertyName = property.getSimpleName();
            String parameterName = decapitalize(propertyName);
            out.println("    this->" + parameterName + " = " + value + " ;");
        }
        out.println("}");
        out.println("");
        out.println("" + className + "::~" + className + "()");
        out.println("{");
        out.println("}");
        out.println("");
        out.println("unsigned char " + className + "::getDataStructureType()");
        out.println("{");
        out.println("    return " + className + "::TYPE ; ");
        out.println("}");
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            String type = toCppType(property.getType());
            String propertyName = property.getSimpleName();
            String parameterName = decapitalize(propertyName);
            out.println("");
            out.println("        ");
            out.println("" + type + " " + className + "::get" + propertyName + "()");
            out.println("{");
            out.println("    return " + parameterName + " ;");
            out.println("}");
            out.println("");
            out.println("void " + className + "::set" + propertyName + "(" + type + " " + parameterName + ")");
            out.println("{");
            out.println("    this->" + parameterName + " = " + parameterName + " ;");
            out.println("}");
        }
        out.println("");
        out.println("int " + className + "::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw (IOException)");
        out.println("{");
        out.println("    int size = 0 ;");
        out.println("");
        out.println("    size += " + baseClass + "::marshal(marshaller, mode, ostream) ; ");

        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            String marshalMethod = toMarshalMethodName(property.getType());
            String propertyName = decapitalize(property.getSimpleName());
            out.println("    size += marshaller->" + marshalMethod + "(" + propertyName + ", mode, ostream) ; ");
        }
        out.println("    return size ;");
        out.println("}");
        out.println("");
        out.println("void " + className + "::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw (IOException)");
        out.println("{");
        out.println("    " + baseClass + "::unmarshal(marshaller, mode, istream) ; ");
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            String cast = toUnmarshalCast(property.getType());
            String unmarshalMethod = toUnmarshalMethodName(property.getType());
            String propertyName = decapitalize(property.getSimpleName());
            out.println("    " + propertyName + " = " + cast + "(marshaller->" + unmarshalMethod + "(mode, istream)) ; ");
        }
        out.println("}");
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

}
