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
#ifndef Ppr_ICharsetEncoder_hpp_
#define Ppr_ICharsetEncoder_hpp_

#include <string>
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      namespace encoding
      {
        using namespace ifr ;
        using namespace std ;

/*
 * The ICharsetEncoder interface should be implemented by any class
 * intended to be a character set encoder/decoder.
 */
struct ICharsetEncoder : Interface
{
    virtual int length(p<string> str) = 0 ;
    virtual p<string> encode(p<string> str, int *enclen) = 0 ;
    virtual p<string> decode(p<string> str) = 0 ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*Ppr_ICharsetEncoder_hpp_*/
