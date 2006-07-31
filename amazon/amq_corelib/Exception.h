/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef ACTIVEMQ_EXCEPTION_H
#define ACTIVEMQ_EXCEPTION_H

#include <exception>
#include <string>

namespace ActiveMQ {
    /// Messaging library exception
    /**
       This class represents an error inside the messaging
       library.  This could be the result of bad function call
       arguments, or a malformed/error message received back from
       the broker.

       @version $Id$
    */
    class Exception : public std::exception {
    public:
        Exception(const std::string& desc) : desc_(desc) {}
        Exception(const char *desc) throw() : desc_(desc) {}
        Exception(const Exception& oth) : desc_(oth.desc_) {}
        Exception& operator=(const Exception& oth) { desc_ = oth.desc_;
                                                     return *this; }
        const char *what() const throw() {return desc_.c_str();}
        virtual ~Exception() throw() {}
    private:
        std::string desc_;
    };
};

#endif // ACTIVEMQ_EXCEPTION_H
