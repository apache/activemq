/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ACTIVEMQ_UTIL_SIMPLEPROPERTIES_H_
#define ACTIVEMQ_UTIL_SIMPLEPROPERTIES_H_

#include <map>
#include <string>
#include <activemq/util/Properties.h>

namespace activemq{
namespace util{
    
    /**
     * Basic implementation of the Properties interface.
     */
    class SimpleProperties : public Properties{
    private:
    
        std::map< std::string, std::string > properties;
        
    public:
    
        virtual ~SimpleProperties(){}
        
        /**
         * Returns true if the properties object is empty
         * @return true if empty
         */
        virtual bool isEmpty() const {
            return properties.empty();
        }

        /**
         * Looks up the value for the given property.
         * @param name The name of the property to be looked up.
         * @return the value of the property with the given name, if it
         * exists.  If it does not exist, returns NULL.
         */
        virtual const char* getProperty( const std::string& name ) const{
            
            std::map< std::string, std::string >::const_iterator iter = 
            properties.find( name );
            if( iter == properties.end() ){
                return NULL;
            }
            
            return iter->second.c_str();
        }
        
        /**
         * Looks up the value for the given property.
         * @param name the name of the property to be looked up.
         * @param defaultValue The value to be returned if the given
         * property does not exist.
         * @return The value of the property specified by <code>name</code>, if it
         * exists, otherwise the <code>defaultValue</code>.
         */
        virtual std::string getProperty( const std::string& name, 
                                         const std::string& defaultValue ) const {
            
            std::map< std::string, std::string >::const_iterator iter = 
            properties.find( name );
            if( iter == properties.end() ){
                return defaultValue;
            }
            
            return iter->second;
        }
        
        /**
         * Sets the value for a given property.  If the property already
         * exists, overwrites the value.
         * @param name The name of the value to be written.
         * @param value The value to be written.
         */
        virtual void setProperty( const std::string& name, 
                                  const std::string& value ){
            properties[name] = value;
        }
        
      /**
       * Check to see if the Property exists in the set
       * @return true if property exists, false otherwise.
       */
      virtual bool hasProperty( const std::string& name ) const
      {
         if(properties.find(name) != properties.end())
         {
            return true;
         }
         
         return false;
      }

        /**
         * Method that serializes the contents of the property map to
         * an arryay.
         * @return list of pairs where the first is the name and the second
         * is the value.
         */
        virtual std::vector< std::pair< std::string, std::string > > toArray() const{
            
            // Create a vector big enough to hold all the elements in the map.
            std::vector< std::pair<std::string, std::string> > vec( properties.size() );
            
            // Get an iterator at the beginning of the map.
            std::map< std::string, std::string >::const_iterator iter = properties.begin();
            
            // Copy all of the elements from the map to the vector.
            for( int ix=0; iter != properties.end(); ++iter, ++ix ){
                vec[ix] = *iter;
            }
            
            return vec;
        }
        
        /**
         * Copies the contents of the given properties object to this one.
         * @param source The source properties object.
         */
        virtual void copy( const Properties* source ){
            
            clear();
            
            std::vector< std::pair< std::string, std::string > > vec =
                source->toArray();
            for( unsigned int ix=0; ix<vec.size(); ++ix ){
                properties[vec[ix].first] = vec[ix].second;
            }
        }
        
        /**
         * Clones this object.
         * @returns a replica of this object.
         */
        virtual Properties* clone() const{
            
            SimpleProperties* props = new SimpleProperties();
            
            props->properties = properties;
            
            return props;
        }
        
        /**
         * Clears all properties from the map.
         */
        virtual void clear(){
            properties.clear();
        }
    };
    
}}

#endif /*ACTIVEMQ_UTIL_SIMPLEPROPERTIES_H_*/
