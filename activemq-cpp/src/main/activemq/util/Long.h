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

#ifndef _ACTIVEMQ_UTIL_LONG_H_
#define _ACTIVEMQ_UTIL_LONG_H_

#include <activemq/util/Number.h>

namespace activemq{
namespace util{

    class Long : public Number
    {
    public:

    	Long(void) {}
    	virtual ~Long(void) {}

        /**
         * Parses the String passed and extracts an long.
         * @param String to parse
         * @return long value
         */
        static long parseLong(const std::string& value){
            long ret = 0;
            std::istringstream istream(value);
            istream.clear();
            istream >> ret;
            return ret;
        }
        
        /**
         * Converts the long to a String representation
         * @param long to convert
         * @return string representation
         */
        static std::string toString(long value){
            std::ostringstream ostream;
            ostream << value;
            return ostream.str();
        }
    };

}}

#endif /*_ACTIVEMQ_UTIL_LONG_H_*/
