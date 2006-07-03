/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ACTIVEMQ_UTIL_PROPERTIES_H_
#define ACTIVEMQ_UTIL_PROPERTIES_H_

#include <map>
#include <string>
#include <vector>

namespace activemq{
namespace util{
   
   /**
    * Interface for a Java-like properties object.  This is essentially 
    * a map of key-value string pairs.
    */
   class Properties{
   public:
   
      virtual ~Properties(){}
      
      /**
       * Looks up the value for the given property.
       * @param name The name of the property to be looked up.
       * @return the value of the property with the given name, if it
       * exists.  If it does not exist, returns NULL.
       */
      virtual const char* getProperty( const std::string& name ) const = 0;
      
      /**
       * Looks up the value for the given property.
       * @param name the name of the property to be looked up.
       * @param defaultValue The value to be returned if the given
       * property does not exist.
       * @return The value of the property specified by <code>name</code>, if it
       * exists, otherwise the <code>defaultValue</code>.
       */
      virtual std::string getProperty( const std::string& name, 
         const std::string& defaultValue ) const = 0;
      
      /**
       * Sets the value for a given property.  If the property already
       * exists, overwrites the value.
       * @param name The name of the value to be written.
       * @param value The value to be written.
       */
      virtual void setProperty( const std::string& name, 
         const std::string& value ) = 0;
      
      /**
       * Check to see if the Property exists in the set
       * @return true if property exists, false otherwise.
       */
      virtual bool hasProperty( const std::string& name ) const = 0;

      /**
       * Method that serializes the contents of the property map to
       * an arryay.
       * @return list of pairs where the first is the name and the second
       * is the value.
       */
      virtual std::vector< std::pair<std::string, std::string> > toArray() const = 0;
      
      /**
       * Copies the contents of the given properties object to this one.
       * @param source The source properties object.
       */
      virtual void copy( const Properties* source ) = 0;
      
      /**
       * Clones this object.
       * @returns a replica of this object.
       */
      virtual Properties* clone() const = 0;
      
      /**
       * Clears all properties from the map.
       */
      virtual void clear() = 0;
   };
   
}}

#endif /*ACTIVEMQ_UTIL_PROPERTIES_H_*/
