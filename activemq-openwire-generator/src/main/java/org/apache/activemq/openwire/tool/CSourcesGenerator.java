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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

/**
 * 
 * @version $Revision: 383749 $
 */
public class CSourcesGenerator extends CHeadersGenerator {

	public Object run() {
		filePostFix = ".c";
		if (destFile == null) {
			destFile = new File(targetDir + "/ow_commands_v" + getOpenwireVersion() + ".c");
		}
		return super.run();
	}

	protected List sort(List source) {
		return source;
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
		out.println("");
		out.println("");
		out.println("#include \"ow_commands_v"+openwireVersion+".h\"");
		out.println("");
		out.println("#define SUCCESS_CHECK( f ) { apr_status_t rc=f; if(rc!=APR_SUCCESS) return rc; }");
		out.println("");
	}

	protected void generateFile(PrintWriter out) throws Exception {
    
        ArrayList properties = new ArrayList();
        jclass.getDeclaredProperties();
        for (int i = 0; i < jclass.getDeclaredProperties().length; i++) {
            JProperty p = jclass.getDeclaredProperties()[i];
            if (isValidProperty(p)) {
                properties.add(p);
            }
        }
        
		String name = jclass.getSimpleName();
		String type = ("ow_"+name).toUpperCase()+"_TYPE";
		String baseName = "DataStructure";
		JClass superclass = jclass.getSuperclass();	            
		while( superclass.getSuperclass() != null ) {
		   if( sortedClasses.contains(superclass) ) {
		      baseName = superclass.getSimpleName();
		      break;
		   } else {
		      superclass = superclass.getSuperclass();
		   }
		}
               
out.println("ow_boolean ow_is_a_"+name+"(ow_DataStructure *object) {");
out.println("   if( object == 0 )");
out.println("      return 0;");
out.println("      ");
out.println("   switch(object->structType) {");

		for (Iterator iterator = sortedClasses.iterator(); iterator.hasNext();) {
			JClass sub = (JClass) iterator.next();
			String subtype = "OW_"+sub.getSimpleName().toUpperCase()+"_TYPE";
			if( jclass.isAssignableFrom(sub) && !isAbstract(sub) ) {
out.println("");
out.println("   case "+subtype+":");
			}            
		}
out.println("");
out.println("      return 1;");
out.println("   }");
out.println("   return 0;");
out.println("}");
               
		if( !isAbstract(jclass) ) {
out.println("");
out.println("");
out.println("ow_"+name+" *ow_"+name+"_create(apr_pool_t *pool) ");
out.println("{");
out.println("   ow_"+name+" *value = apr_pcalloc(pool,sizeof(ow_"+name+"));");
out.println("   if( value!=0 ) {");
out.println("      ((ow_DataStructure*)value)->structType = "+type+";");
out.println("   }");
out.println("   return value;");
out.println("}");
        }
               
out.println("");
out.println("");
out.println("apr_status_t ow_marshal1_"+name+"(ow_bit_buffer *buffer, ow_"+name+" *object)");
out.println("{");
out.println("   ow_marshal1_"+baseName+"(buffer, (ow_"+baseName+"*)object);");

		for (Iterator iter = properties.iterator(); iter.hasNext();) {
			JProperty property = (JProperty) iter.next();
			String propname = toPropertyCase(property.getSimpleName());
			boolean cached = isCachedProperty(property);
			JAnnotation annotation = property.getGetter().getAnnotation("openwire:property");
			JAnnotationValue size = annotation.getValue("size");

           type = property.getType().getQualifiedName();
           if( type.equals("boolean")) {
out.println("   ow_bit_buffer_append(buffer, object->"+propname+");");
                   } else if( type.equals("byte")) {
                   } else if( type.equals("char")) {
                   } else if( type.equals("short")) {
                   } else if( type.equals("int")) {
                   } else if( type.equals("long")) {
out.println("   ow_marshal1_long(buffer, object->"+propname+");");
                   } else if( type.equals("byte[]")) {
                     if( size ==null ) {
out.println("   ow_bit_buffer_append(buffer,  object->"+propname+"!=0 );");
                     }
                   } else if( type.equals("org.apache.activeio.packet.ByteSequence")) {
                     if( size ==null ) {
out.println("   ow_bit_buffer_append(buffer,  object->"+propname+"!=0 );");
                     }
                   } else if( type.equals("java.lang.String")) {
out.println("   ow_marshal1_string(buffer, object->"+propname+");");
                   } else {
                         if( property.getType().isArrayType() ) {
                           if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_marshal1_DataStructure_array_const_size(buffer, object->"+propname+", "+size.asInt()+"));");
                           } else {
out.println("   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->"+propname+"));");
                           }
                         } else if( isThrowable(property.getType()) ) {    	    
out.println("   SUCCESS_CHECK(ow_marshal1_throwable(buffer, object->"+propname+"));");
                         } else {
                           if( cached ) {
out.println("   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->"+propname+"));");
                           } else {
out.println("   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->"+propname+"));");
                           }
                       }
                   }
out.println("");
               }


out.println("   ");
out.println("	return APR_SUCCESS;");
out.println("}");
out.println("apr_status_t ow_marshal2_"+name+"(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_"+name+" *object)");
out.println("{");
out.println("   ow_marshal2_"+baseName+"(buffer, bitbuffer, (ow_"+baseName+"*)object);   ");

			for (Iterator iter = properties.iterator(); iter.hasNext();) {
				JProperty property = (JProperty) iter.next();
                   JAnnotation annotation = property.getGetter().getAnnotation("openwire:property");
                   JAnnotationValue size = annotation.getValue("size");
                   Object propname = toPropertyCase(property.getSimpleName());
                   boolean cached = isCachedProperty(property);
                   
                   type = property.getType().getQualifiedName();
                   if( type.equals("boolean") ) {
out.println("   ow_bit_buffer_read(bitbuffer);");
                   } else if( type.equals("byte") ) {
out.println("   SUCCESS_CHECK(ow_byte_buffer_append_"+type+"(buffer, object->"+propname+"));");
                   } else if( type.equals("char") ) {
out.println("   SUCCESS_CHECK(ow_byte_buffer_append_"+type+"(buffer, object->"+propname+"));");
                   } else if( type.equals("short") ) {
out.println("   SUCCESS_CHECK(ow_byte_buffer_append_"+type+"(buffer, object->"+propname+"));");
                   } else if( type.equals("int") ) {
out.println("   SUCCESS_CHECK(ow_byte_buffer_append_"+type+"(buffer, object->"+propname+"));");
                   } else if( type.equals("long") ) {
out.println("   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->"+propname+"));");
                   } else if( type.equals("byte[]") ) {
                       if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_marshal2_byte_array_const_size(buffer, object->"+propname+", "+size.asInt()+"));");
                       } else {
out.println("   SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->"+propname+"));");
                       }
                   } else if( type.equals("org.apache.activeio.packet.ByteSequence") ) {
                       if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_marshal2_byte_array_const_size(buffer, object->"+propname+", "+size.asInt()+"));");
                       } else {
out.println("   SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->"+propname+"));");
                       }
                   } else if( type.equals("java.lang.String") ) {
out.println("   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->"+propname+"));");
                   } else {
                      if( property.getType().isArrayType() ) {
                         if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_marshal2_DataStructure_array_const_size(buffer, bitbuffer, object->"+propname+", "+size.asInt()+"));");
                         } else {
out.println("   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->"+propname+"));");
                         }
                      } else if( isThrowable(property.getType()) ) {    	    
out.println("   SUCCESS_CHECK(ow_marshal2_throwable(buffer, bitbuffer, object->"+propname+"));");
                      } else {
                           if( cached ) {
out.println("   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->"+propname+"));");
                           } else {
out.println("   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->"+propname+"));");
                           }                      
                      }
                   }
out.println("");
               }

out.println("   ");
out.println("	return APR_SUCCESS;");
out.println("}");
out.println("");
out.println("apr_status_t ow_unmarshal_"+name+"(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_"+name+" *object, apr_pool_t *pool)");
out.println("{");
out.println("   ow_unmarshal_"+baseName+"(buffer, bitbuffer, (ow_"+baseName+"*)object, pool);   ");

	for (Iterator iter = properties.iterator(); iter.hasNext();) {
		JProperty property = (JProperty) iter.next();
                   JAnnotation annotation = property.getGetter().getAnnotation("openwire:property");
                   JAnnotationValue size = annotation.getValue("size");
                   String propname = toPropertyCase(property.getSimpleName());
                   boolean cached = isCachedProperty(property);
                   
                   type = property.getType().getQualifiedName();

                   if( type.equals("boolean") ) {
out.println("   object->"+propname+" = ow_bit_buffer_read(bitbuffer);");
                   } else if( type.equals("byte") ) {
out.println("   SUCCESS_CHECK(ow_byte_array_read_"+type+"(buffer, &object->"+propname+"));");
                   } else if( type.equals("char") ) {
out.println("   SUCCESS_CHECK(ow_byte_array_read_"+type+"(buffer, &object->"+propname+"));");
                   } else if( type.equals("short") ) {
out.println("   SUCCESS_CHECK(ow_byte_array_read_"+type+"(buffer, &object->"+propname+"));");
                   } else if( type.equals("int") ) {
out.println("   SUCCESS_CHECK(ow_byte_array_read_"+type+"(buffer, &object->"+propname+"));");
                   } else if( type.equals("long") ) {
out.println("   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->"+propname+", pool));");
                   } else if( type.equals("byte[]") ) {
                       if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_unmarshal_byte_array_const_size(buffer, &object->"+propname+", "+size.asInt()+", pool));");
                       } else {
out.println("   SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->"+propname+", pool));");
                       }
                   } else if( type.equals("org.apache.activeio.packet.ByteSequence") ) {
                       if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_unmarshal_byte_array_const_size(buffer, &object->"+propname+", "+size.asInt()+", pool));");
                       } else {
out.println("   SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->"+propname+", pool));");
                       }
                   } else if( type.equals("java.lang.String") ) {
out.println("   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->"+propname+", pool));");
                   } else {
                      if( property.getType().isArrayType() ) {
                        if( size!=null ) {
out.println("   SUCCESS_CHECK(ow_unmarshal_DataStructure_array_const_size(buffer, bitbuffer, &object->"+propname+", "+size.asInt()+", pool));");
                        } else {
out.println("   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->"+propname+", pool));");
                        }
                      } else if( isThrowable(property.getType()) ) {    	    
out.println("   SUCCESS_CHECK(ow_unmarshal_throwable(buffer, bitbuffer, &object->"+propname+", pool));");
                      } else {
                           if( cached ) {
out.println("   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->"+propname+", pool));");
                           } else {
out.println("   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->"+propname+", pool));");
                           }                      
                      }
                   }
out.println("");
               }

out.println("   ");
out.println("	return APR_SUCCESS;");
out.println("}");
         }  
         
	protected void generateTearDown(PrintWriter out) {
out.println("");
out.println("ow_DataStructure *ow_create_object(ow_byte type, apr_pool_t *pool)");
out.println("{");
out.println("   switch( type ) {");
		for (Iterator iterator = sortedClasses.iterator(); iterator.hasNext();) {
			JClass jclass = (JClass) iterator.next();
            String name = jclass.getSimpleName();
            String type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out.println("      case "+type+": return (ow_DataStructure *)ow_"+name+"_create(pool);");
            }
         }
         
out.println("");
out.println("   }");
out.println("   return 0;");
out.println("}");
out.println("");
out.println("apr_status_t ow_marshal1_object(ow_bit_buffer *buffer, ow_DataStructure *object)");
out.println("{");
out.println("   switch( object->structType ) {");

		for (Iterator iterator = sortedClasses.iterator(); iterator.hasNext();) {
			JClass jclass = (JClass) iterator.next();
            String name = jclass.getSimpleName();
            String type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out.println("      case "+type+": return ow_marshal1_"+name+"(buffer, (ow_"+name+"*)object);");
            }
         }
         
out.println("");
out.println("   }");
out.println("   return APR_EGENERAL;");
out.println("}");
out.println("");
out.println("apr_status_t ow_marshal2_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object)");
out.println("{");
out.println("   switch( object->structType ) {");

		for (Iterator iterator = sortedClasses.iterator(); iterator.hasNext();) {
			JClass jclass = (JClass) iterator.next();
            String name = jclass.getSimpleName();
            String type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out.println("      case "+type+": return ow_marshal2_"+name+"(buffer, bitbuffer, (ow_"+name+"*)object);");
            }
         }

out.println("");
out.println("   }");
out.println("   return APR_EGENERAL;");
out.println("}");
out.println("");
out.println("apr_status_t ow_unmarshal_object(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object, apr_pool_t *pool)");
out.println("{");
out.println("   switch( object->structType ) {");

		for (Iterator iterator = sortedClasses.iterator(); iterator.hasNext();) {
			JClass jclass = (JClass) iterator.next();
            String name = jclass.getSimpleName();
            String type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out.println("      case "+type+": return ow_unmarshal_"+name+"(buffer, bitbuffer, (ow_"+name+"*)object, pool);");
            }
         }

out.println("");
out.println("   }");
out.println("   return APR_EGENERAL;");
out.println("}");
 
	}
}
