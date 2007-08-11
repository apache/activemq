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
package org.apache.activemq.openwire.tool;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

/**
 * 
 * @version $Revision: 383749 $
 */
public class CHeadersGenerator extends SingleSourceGenerator {

    protected String targetDir = "./src/lib/openwire";

    public Object run() {
        filePostFix = ".h";
        if (destFile == null) {
            destFile = new File(targetDir + "/ow_commands_v" + getOpenwireVersion() + ".h");
        }
        return super.run();
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    protected void generateLicence(PrintWriter out) {
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

    String changeCase(String value) {
        StringBuffer b = new StringBuffer();
        char[] cs = value.toCharArray();
        for (int i = 0; i < cs.length; i++) {
            char c = cs[i];
            if (Character.isUpperCase((char) c)) {
                b.append('_');
                b.append(Character.toLowerCase((char) c));
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }

    String toPropertyCase(String value) {
        return value.substring(0, 1).toLowerCase() + value.substring(1);
    }

    /**
     * Sort the class list so that base classes come up first.
     */
    protected List<JClass> sort(List source) {
        LinkedHashMap<JClass, JClass> rc = new LinkedHashMap<JClass, JClass>();
    	ArrayList classes = new ArrayList(source);
        Collections.sort(classes, new Comparator(){
			public int compare(Object o1, Object o2) {
				JClass c1 = (JClass) o1;
				JClass c2 = (JClass) o2;
				return c1.getSimpleName().compareTo(c2.getSimpleName());
			}});
    	
        // lets make a map of all the class names
        HashMap<JClass, JClass> classNames = new HashMap<JClass, JClass>();
        for (Iterator iter = classes.iterator(); iter.hasNext();) {
            JClass c = (JClass) iter.next();
            classNames.put(c, c);
        }

        // Add all classes that have no parent first
        for (Iterator iter = classes.iterator(); iter.hasNext();) {
            JClass c = (JClass) iter.next();
            if (!classNames.containsKey(c.getSuperclass()))
                rc.put(c, c);
        }

        // now lets add the rest
        for (Iterator iter = classes.iterator(); iter.hasNext();) {
            JClass c = (JClass) iter.next();
            if (!rc.containsKey(c))
                rc.put(c,c);
        }

        return new ArrayList<JClass>(rc.keySet());
    }

    void generateFields(PrintWriter out, JClass jclass) {

        if (jclass.getSuperclass() == null || jclass.getSuperclass().getSimpleName().equals("Object")) {
            out.println("");
            out.println("   ow_byte structType;");
        } else {
            generateFields(out, jclass.getSuperclass());
        }

        ArrayList<JProperty> properties = new ArrayList<JProperty>();
        jclass.getDeclaredProperties();
        for (int i = 0; i < jclass.getDeclaredProperties().length; i++) {
            JProperty p = jclass.getDeclaredProperties()[i];
            if (isValidProperty(p)) {
                properties.add(p);
            }
        }
        for (Iterator<JProperty> iter = properties.iterator(); iter.hasNext();) {
            JProperty property = iter.next();
            JAnnotation annotation = property.getGetter().getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            String name = toPropertyCase(property.getSimpleName());
            boolean cached = isCachedProperty(property);

            String type = property.getType().getQualifiedName();
            if (type.equals("boolean")) {
                out.println("   ow_"+type+" "+name+";");
            } else if (type.equals("byte")) {
                out.println("   ow_"+type+" "+name+";");
            } else if (type.equals("char")) {
                out.println("   ow_"+type+" "+name+";");
            } else if (type.equals("short")) {
                out.println("   ow_"+type+" "+name+";");
            } else if (type.equals("int")) {
                out.println("   ow_"+type+" "+name+";");
            } else if (type.equals("long")) {
                out.println("   ow_"+type+" "+name+";");
            } else if (type.equals("byte[]")) {
                out.println("   ow_byte_array *"+name+";");
            } else if (type.equals("org.apache.activeio.packet.ByteSequence")) {
                out.println("   ow_byte_array *"+name+";");
            } else if (type.equals("org.apache.activeio.packet.ByteSequence")) {
                out.println("   ow_byte_array *"+name+";");
            } else if (type.equals("java.lang.String")) {
                out.println("   ow_string *"+name+";");
            } else {
                if (property.getType().isArrayType()) {
                    out.println("   ow_DataStructure_array *"+name+";");
                } else if (isThrowable(property.getType())) {
                    out.println("   ow_throwable *"+name+";");
                } else {
                    out.println("   struct ow_" + property.getType().getSimpleName() + " *"+name+";");
                }
            }
        }
    }
    
    
    protected void generateSetup(PrintWriter out) {
        generateLicence(out);
out.println("");
out.println("/*****************************************************************************************");
out.println(" *  ");
out.println(" * NOTE!: This file is auto generated - do not modify!");
out.println(" *        if you need to make a change, please see the modify the groovy scripts in the");
out.println(" *        under src/gram/script and then use maven openwire:generate to regenerate ");
out.println(" *        this file.");
out.println(" *  ");
out.println(" *****************************************************************************************/");
out.println(" ");
out.println("#ifndef OW_COMMANDS_V"+openwireVersion+"_H");
out.println("#define OW_COMMANDS_V"+openwireVersion+"_H");
out.println("");
out.println("#include \"ow.h\"");
out.println("");
out.println("#ifdef __cplusplus");
out.println("extern \"C\" {");
out.println("#endif /* __cplusplus */");
out.println("      ");
out.println("#define OW_WIREFORMAT_VERSION "+openwireVersion+"");

out.println("#define OW_WIREFORMAT_STACK_TRACE_MASK     0x00000001;");
out.println("#define OW_WIREFORMAT_TCP_NO_DELAY_MASK    0x00000002;");
out.println("#define OW_WIREFORMAT_CACHE_MASK           0x00000004;");
out.println("#define OW_WIREFORMAT_COMPRESSION_MASK     0x00000008;");

		for (Iterator iterator = sortedClasses.iterator(); iterator.hasNext();) {
			JClass jclass = (JClass) iterator.next();
		    String name = jclass.getSimpleName();
		    String type = ("ow_"+name).toUpperCase()+"_TYPE";
		    if( !isAbstract(jclass) ) {
		    	out.println("#define "+type+" "+getOpenWireOpCode(jclass));
		    }
		 }

out.println("      ");
out.println("apr_status_t ow_bitmarshall(ow_bit_buffer *buffer, ow_DataStructure *object);");
out.println("apr_status_t ow_marshall(ow_byte_buffer *buffer, ow_DataStructure *object);");
    }
    
    protected void generateFile(PrintWriter out) throws Exception {

        String structName = jclass.getSimpleName();

out.println("");
out.println("typedef struct ow_"+structName+" {");

        // This recusivly generates the field definitions of the class and it's supper classes.
        generateFields(out, jclass);

out.println("");
out.println("} ow_"+structName+";");
out.println("ow_"+structName+" *ow_"+structName+"_create(apr_pool_t *pool);");
out.println("ow_boolean ow_is_a_"+structName+"(ow_DataStructure *object);");

    }
    
    protected void generateTearDown(PrintWriter out) {
out.println("");
out.println("#ifdef __cplusplus");
out.println("}");
out.println("#endif");
out.println("");
out.println("#endif  /* ! OW_COMMANDS_V"+openwireVersion+"_H */");
    }
}
