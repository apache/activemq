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
#ifndef Ppr_SocketException_hpp_
#define Ppr_SocketException_hpp_

#include <stdexcept>

namespace apache
{
  namespace ppr
  {
    namespace net
    {
      using namespace std;

/**
 */
class SocketException : public exception
{
protected:
    string msg;
public:
    SocketException(const char* message) : msg (message) {};
    virtual ~SocketException() throw() {}
    virtual const char* what() const throw () {
      return msg.c_str();
    }
} ;


/* namespace */
    }
  }
}

#endif /*Ppr_SocketException_hpp_*/

