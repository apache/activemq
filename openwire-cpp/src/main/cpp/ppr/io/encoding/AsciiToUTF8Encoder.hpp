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
#ifndef Ppr_AsciiToUTF8Encoder_hpp_
#define Ppr_AsciiToUTF8Encoder_hpp_

#include <string>
#include <ppr/io/ByteArrayOutputStream.hpp>
#include <ppr/io/encoding/ICharsetEncoder.hpp>
#include <ppr/io/encoding/CharsetEncodingException.hpp>
#include <ppr/util/ifr/array>
#include <ppr/util/ifr/p>

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      namespace encoding
      {
        using namespace std;
        using namespace ifr;
        using namespace ppr::io ;

/*
 * Character encoder for extended ASCII to modified UTF-8 encoding.
 */
class AsciiToUTF8Encoder : public ICharsetEncoder
{
private:

public:
    static const char* NAME ;

public:
    AsciiToUTF8Encoder() ;
    virtual ~AsciiToUTF8Encoder() ;

    virtual int length(p<string> str) ;
    virtual p<string> encode(p<string> str, int *enclen) throw (CharsetEncodingException) ;
    virtual p<string> decode(p<string> str) throw (CharsetEncodingException) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*Ppr_AsciiToUTF8Encoder_hpp_*/
