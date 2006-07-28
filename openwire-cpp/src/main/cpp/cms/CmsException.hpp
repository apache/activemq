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
#ifndef Cms_CmsException_hpp_
#define Cms_CmsException_hpp_

#include <exception>
#include <string>

namespace apache
{
  namespace cms
  {
    using namespace std;

/*
 * 
 */
class CmsException : public exception
{
protected:
    string msg;

public:
    CmsException() ;
    CmsException(const char* message) ;
    virtual ~CmsException() throw();

    virtual const char* what() const throw () {
      return msg.c_str();
    }
} ;

/* namespace */
  }
}

#endif /*Cms_CmsException_hpp_*/
