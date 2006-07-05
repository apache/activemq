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
#ifndef _ACTIVEMQ_LOGGER_PROPERTIESCHANGELISTENER_H_
#define _ACTIVEMQ_LOGGER_PROPERTIESCHANGELISTENER_H_

namespace activemq{
namespace logger{

   /**
    * Defines the interface that classes can use to listen for change
    * events on Properties.
    */
   class PropertiesChangeListener
   {
   public:
   
      /**
       * Destructor
       */
      virtual ~PropertiesChangeListener() {}
      
      /**
       * Change Event, called when a property is changed
       * @param Name of the Property
       * @param Old Value of the Property
       * @param New Value of the Property
       */
      virtual void onPropertyChanged(const std::string& name,
                                     const std::string& oldValue,
                                     const std::string& newValue) = 0;

   };

}}

#endif /*_ACTIVEMQ_LOGGER_PROPERTIESCHANGELISTENER_H_*/
