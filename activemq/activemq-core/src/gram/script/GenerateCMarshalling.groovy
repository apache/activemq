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
import org.apache.activemq.openwire.tool.OpenWireScript

/**
 * Generates the Java marshalling code for the Open Wire Format
 *
 * @version $Revision$
 */
class GenerateCMarshalling extends OpenWireScript {

	String changeCase(String value) {
		StringBuffer b = new StringBuffer();
		value.each { c |
			if( Character.isUpperCase((char)c) ) {
				b.append('_');
				b.append(Character.toLowerCase((char)c));
			} else {
				b.append(c);
			}
		}
		return b.toString();
	}
   
	String toPropertyCase(String value) {
      return value.substring(0,1).toLowerCase()+value.substring(1);
	}
	
	/**
	 * Sort the class list so that base classes come up first.
	 */
	def sort(classes) {
   
      classes = (java.util.List)classes;      
	  def rc = [];
      def objectClass;
     
      // lets make a map of all the class names
      def classNames = [:]
	 for (c in classes) {
	     def name = c.simpleName
	     classNames[name] = name
	 }
      
      // Add all classes that have no parent first
     for (c in classes) {
       if( !classNames.containsKey(c.superclass))
          rc.add(c)
      }
      
      // now lets add the rest
     for (c in classes) {
         if (!rc.contains(c))
             rc.add(c)
      }
      
	return rc;
	}
   
   
   def generateFields(out, jclass) {
   
//      println("getting fields for: ${jclass.simpleName}");
      if( jclass.superclass == null || jclass.superclass.simpleName.equals("Object") ) {
out << """
   ow_byte structType;
""";
      } else {
         generateFields(out, jclass.superclass);
      }

      def properties = jclass.declaredProperties.findAll { isValidProperty(it) }
      for (property in properties) {
          def annotation = property.getter.getAnnotation("openwire:property");
          def size = annotation.getValue("size");
          def name = toPropertyCase(property.simpleName);
          def cached = isCachedProperty(property);
          
          out << "   "
          def type = property.type.qualifiedName
          switch (type) {
          case "boolean":
              out << "ow_$type $name;"; break;
              break;
          case "byte":
              out << "ow_$type $name;"; break;
              break;
          case "char":
              out << "ow_$type $name;"; break;
              break;
          case "short":
              out << "ow_$type $name;"; break;
              break;
          case "int":
              out << "ow_$type $name;"; break;
              break;
          case "long":
              out << "ow_$type $name;"; break;
              break;
          case "byte[]":
              out << "ow_byte_array *$name;"; break;
              break;
          case "org.activeio.ByteSequence":
              out << "ow_byte_array *$name;"; break;
              break;
	      case "org.activeio.ByteSequence":
              out << "ow_byte_array *$name;"; break;
              break;
          case "java.lang.String":
              out << "ow_string *$name;"; break;
              break;
          default:
                if( property.type.arrayType ) {
                  out << "ow_DataStructure_array *$name;"; break;
                } else if( isThrowable(property.type) ) {    	    
                  out << "ow_throwable *$name;"; break;
                } else {
                  out << "struct ow_"+property.type.simpleName+" *$name;"; break;
              }
          }
out << """
"""
      }
   
   }

    Object run() {
    
        def openwireVersion = System.getProperty("openwire.version");
        
        def destDir = new File("../openwire-c/src/libopenwire")
        println "Generating C marshalling code to directory ${destDir}"
        
        def openwireClasses = classes.findAll {
        		it.getAnnotation("openwire:marshaller")!=null
        }
        
        println "Sorting classes..."
	     openwireClasses = sort(openwireClasses);
	
        def concreteClasses = new ArrayList()
        int counter = 0
        Map map = [:]

        destDir.mkdirs()
        
      //////////////////////////////////////////////////////////////////////
      //
      // Start generateing the ow_commands_v1.h File
      //
      //////////////////////////////////////////////////////////////////////
        def file = new File(destDir, "ow_commands_v${openwireVersion}.h")
        file.withWriter { out |
        
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

/*****************************************************************************************
 *  
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *  
 *****************************************************************************************/
 
#ifndef OW_COMMANDS_V${openwireVersion}_H
#define OW_COMMANDS_V${openwireVersion}_H

#include "ow.h"
#include "ow_command_types_v${openwireVersion}.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */
      
#define OW_WIREFORMAT_VERSION ${openwireVersion}
      
apr_status_t ow_bitmarshall(ow_bit_buffer *buffer, ow_DataStructure *object);
apr_status_t ow_marshall(ow_byte_buffer *buffer, ow_DataStructure *object);
"""        
           
	        for (jclass in openwireClasses) {
	               
	            println "Processing ${jclass.simpleName}"
	            
	            def structName = jclass.simpleName;
	            def type = "OW_"+structName.toUpperCase()+"_TYPE"
	            
               counter++;
out << """
typedef struct ow_${structName} {
"""
   // This recusivly generates the field definitions of the class and it's supper classes.
   generateFields(out, jclass);
   
out << """
} ow_${structName};
ow_${structName} *ow_${structName}_create(apr_pool_t *pool);
ow_boolean ow_is_a_${structName}(ow_DataStructure *object);
"""
	        }
           
out << """
#ifdef __cplusplus
}
#endif

#endif  /* ! OW_COMMANDS_V${openwireVersion}_H */
"""
           
		}
		
      //////////////////////////////////////////////////////////////////////
      //
      // Start generateing the ow_commands_v1.c File
      //
      //////////////////////////////////////////////////////////////////////
      
      file = new File(destDir, "ow_commands_v${openwireVersion}.c")
      file.withWriter { out |
      
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

/*****************************************************************************************
 *  
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *  
 *****************************************************************************************/


#include "ow_commands_v${openwireVersion}.h"

#define SUCCESS_CHECK( f ) { apr_status_t rc=f; if(rc!=APR_SUCCESS) return rc; }

"""
         for (jclass in openwireClasses) {
	
	            properties = jclass.declaredProperties.findAll { isValidProperty(it) }
	            def name = jclass.simpleName;
	            def type = ("ow_"+name).toUpperCase()+"_TYPE"
	            
	            def baseName="DataStructure";
	            
	            def superclass = jclass.superclass;	            
	            while( superclass.superclass != null ) {
  	              if( openwireClasses.contains(superclass) ) {
                      baseName = superclass.simpleName;
                      break;
                   } else {
                      superclass = superclass.superclass;
                   }
	            }
               
out << """
ow_boolean ow_is_a_${name}(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {"""
            for (sub in openwireClasses) {
	            def subtype = "OW_"+sub.simpleName.toUpperCase()+"_TYPE"
               if( jclass.isAssignableFrom(sub) && !isAbstract(sub) ) {
out << """
   case $subtype:"""
               
               }            
            }
out << """
      return 1;
   }
   return 0;
}
"""
               

               if( !isAbstract(jclass) ) {
out << """

ow_${name} *ow_${name}_create(apr_pool_t *pool) 
{
   ow_${name} *value = apr_pcalloc(pool,sizeof(ow_${name}));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = ${type};
   }
   return value;
}
"""
               }
               
out << """

apr_status_t ow_marshal1_${name}(ow_bit_buffer *buffer, ow_${name} *object)
{
   ow_marshal1_${baseName}(buffer, (ow_${baseName}*)object);
"""

	             properties = jclass.declaredProperties.findAll { isValidProperty(it) }
               for (property in properties) {
                   def propname = toPropertyCase(property.simpleName);
                   def cached = isCachedProperty(property);
                   def annotation = property.getter.getAnnotation("openwire:property");
                   def size = annotation.getValue("size");

                   out << "   ";
                   type = property.type.qualifiedName
                   switch (type) {
                   
                   case "boolean":
                       out << "ow_bit_buffer_append(buffer, object->$propname);"; break;
                       break;
                   case "byte":break;
                   case "char":break;
                   case "short":break;
                   case "int":break;
                   case "long":
                       out << "ow_marshal1_long(buffer, object->$propname);"; break;
                       break;
                   case "byte[]":
                     if( size ==null ) {
                        out << """
                        ow_bit_buffer_append(buffer,  object->$propname!=0 );
                        """;
                     }
                     break;
                   case "org.activeio.ByteSequence":
                     if( size ==null ) {
                        out << """
                        ow_bit_buffer_append(buffer,  object->$propname!=0 );
                        """;
                     }
                     break;
                   case "java.lang.String":
                       out << "ow_marshal1_string(buffer, object->$propname);"; break;
                   default:
                         if( property.type.arrayType ) {
                           if( size!=null ) {
                              out << "SUCCESS_CHECK(ow_marshal1_DataStructure_array_const_size(buffer, object->$propname, ${size.asInt()}));"; break;
                           } else {
                              out << "SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->$propname));"; break;
                           }
                         } else if( isThrowable(property.type) ) {    	    
                           out << "SUCCESS_CHECK(ow_marshal1_throwable(buffer, object->$propname));"; break;
                         } else {
                           if( cached ) {
                              out << "SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->$propname));"; break;
                           } else {
                              out << "SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->$propname));"; break;
                           }
                       }
                   }
out << """
"""
               }


out << """   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_${name}(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_${name} *object)
{
   ow_marshal2_${baseName}(buffer, bitbuffer, (ow_${baseName}*)object);   
"""

	             properties = jclass.declaredProperties.findAll { isValidProperty(it) }
               for (property in properties) {
                   def annotation = property.getter.getAnnotation("openwire:property");
                   def size = annotation.getValue("size");
                   def propname = toPropertyCase(property.simpleName);
                   def cached = isCachedProperty(property);
                   
                   out << "   "
                   type = property.type.qualifiedName
                   switch (type) {                   
                   case "boolean": 
                       out << "ow_bit_buffer_read(bitbuffer);"; break;
                   case "byte":
                       out << "SUCCESS_CHECK(ow_byte_buffer_append_${type}(buffer, object->$propname));"; break;
                       break;
                   case "char":
                       out << "SUCCESS_CHECK(ow_byte_buffer_append_${type}(buffer, object->$propname));"; break;
                       break;
                   case "short":
                       out << "SUCCESS_CHECK(ow_byte_buffer_append_${type}(buffer, object->$propname));"; break;
                       break;
                   case "int":
                       out << "SUCCESS_CHECK(ow_byte_buffer_append_${type}(buffer, object->$propname));"; break;
                       break;
                   case "long":
                       out << "SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->$propname));"; break;
                       break;
                   case "byte[]":
                       if( size!=null ) {
                          out << "SUCCESS_CHECK(ow_marshal2_byte_array_const_size(buffer, object->$propname, ${size.asInt()}));"; break;
                       } else {
                          out << "SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->$propname));"; break;
                       }
                       break;
                   case "org.activeio.ByteSequence":
                       if( size!=null ) {
                          out << "SUCCESS_CHECK(ow_marshal2_byte_array_const_size(buffer, object->$propname, ${size.asInt()}));"; break;
                       } else {
                          out << "SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->$propname));"; break;
                       }
                       break;
                   case "java.lang.String":
                       out << "SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->$propname));"; break;
                       break;
                   default:
                      if( property.type.arrayType ) {
                         if( size!=null ) {
                            out << "SUCCESS_CHECK(ow_marshal2_DataStructure_array_const_size(buffer, bitbuffer, object->$propname, ${size.asInt()}));"; break;
                         } else {
                            out << "SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->$propname));"; break;
                         }
                      } else if( isThrowable(property.type) ) {    	    
                        out << "SUCCESS_CHECK(ow_marshal2_throwable(buffer, bitbuffer, object->$propname));"; break;
                      } else {
                           if( cached ) {
                              out << "SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->$propname));"; break;
                           } else {
                              out << "SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->$propname));"; break;
                           }                      
                      }
                   }
out << """
"""
               }

out << """   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_${name}(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_${name} *object, apr_pool_t *pool)
{
   ow_unmarshal_${baseName}(buffer, bitbuffer, (ow_${baseName}*)object, pool);   
"""

	             properties = jclass.declaredProperties.findAll { isValidProperty(it) }
               for (property in properties) {
                   def annotation = property.getter.getAnnotation("openwire:property");
                   def size = annotation.getValue("size");
                   def propname = toPropertyCase(property.simpleName);
                   def cached = isCachedProperty(property);
                   
                   out << "   "
                   type = property.type.qualifiedName
                   switch (type) {                   
                   case "boolean":
                       out << "object->$propname = ow_bit_buffer_read(bitbuffer);"; break;
                       break;
                   case "byte":
                       out << "SUCCESS_CHECK(ow_byte_array_read_${type}(buffer, &object->$propname));"; break;
                       break;
                   case "char":
                       out << "SUCCESS_CHECK(ow_byte_array_read_${type}(buffer, &object->$propname));"; break;
                       break;
                   case "short":
                       out << "SUCCESS_CHECK(ow_byte_array_read_${type}(buffer, &object->$propname));"; break;
                       break;
                   case "int":
                       out << "SUCCESS_CHECK(ow_byte_array_read_${type}(buffer, &object->$propname));"; break;
                       break;
                   case "long":
                       out << "SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->$propname, pool));"; break;
                       break;
                   case "byte[]":
                       if( size!=null ) {
                           out << "SUCCESS_CHECK(ow_unmarshal_byte_array_const_size(buffer, &object->$propname, ${size.asInt()}, pool));"; break;
                       } else {
                           out << "SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->$propname, pool));"; break;
                       }
                       break;
                   case "org.activeio.ByteSequence":
                       if( size!=null ) {
                           out << "SUCCESS_CHECK(ow_unmarshal_byte_array_const_size(buffer, &object->$propname, ${size.asInt()}, pool));"; break;
                       } else {
                           out << "SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->$propname, pool));"; break;
                       }
                       break;
                   case "java.lang.String":
                       out << "SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->$propname, pool));"; break;
                       break;
                   default:
                      if( property.type.arrayType ) {
                        if( size!=null ) {
                           out << "SUCCESS_CHECK(ow_unmarshal_DataStructure_array_const_size(buffer, bitbuffer, &object->$propname, ${size.asInt()}, pool));"; break;
                        } else {
                           out << "SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->$propname, pool));"; break;
                        }
                      } else if( isThrowable(property.type) ) {    	    
                         out << "SUCCESS_CHECK(ow_unmarshal_throwable(buffer, bitbuffer, &object->$propname, pool));"; break;
                      } else {
                           if( cached ) {
                              out << "SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->$propname, pool));"; break;
                           } else {
                              out << "SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->$propname, pool));"; break;
                           }                      
                      }
                   }
out << """
"""
               }

out << """   
	return APR_SUCCESS;
}
"""
         }  
         
         
out << """
ow_DataStructure *ow_create_object(ow_byte type, apr_pool_t *pool)
{
   switch( type ) {
"""
         for (jclass in openwireClasses) {
            def name = jclass.simpleName;
            def type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out << """
      case ${type}: return (ow_DataStructure *)ow_${name}_create(pool);"""
            }
         }
         
out << """
   }
   return 0;
}

apr_status_t ow_marshal1_object(ow_bit_buffer *buffer, ow_DataStructure *object)
{
   switch( object->structType ) {
"""
         for (jclass in openwireClasses) {
            def name = jclass.simpleName;
            def type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out << """
      case ${type}: return ow_marshal1_${name}(buffer, (ow_${name}*)object);"""
            }
         }
         
out << """
   }
   return APR_EGENERAL;
}

apr_status_t ow_marshal2_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object)
{
   switch( object->structType ) {
"""
         for (jclass in openwireClasses) {
            def name = jclass.simpleName;
            def type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out << """
      case ${type}: return ow_marshal2_${name}(buffer, bitbuffer, (ow_${name}*)object);"""
            }
         }

out << """
   }
   return APR_EGENERAL;
}

apr_status_t ow_unmarshal_object(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object, apr_pool_t *pool)
{
   switch( object->structType ) {
"""
         for (jclass in openwireClasses) {
            def name = jclass.simpleName;
            def type = ("ow_"+name).toUpperCase()+"_TYPE";
            if( !isAbstract(jclass) ) {
out << """
      case ${type}: return ow_unmarshal_${name}(buffer, bitbuffer, (ow_${name}*)object, pool);"""
            }
         }

out << """
   }
   return APR_EGENERAL;
}
"""
      }
   }
}
