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
#include "ppr/TraceException.hpp"

using namespace apache::ppr;

/*
 * 
 */
TraceException::TraceException()
   : exception()
{
    // no-op
} ;

/*
 * 
 */
TraceException::TraceException(const char* msg, ...)
{
    va_list vargs ;

    va_start(vargs, msg) ;
    buildMessage(msg, vargs) ;
}

void TraceException::buildMessage(const char* format, va_list& vargs)
{
    // Allocate buffer with a guess of it's size
    array<char> buffer (128);

    // Format string
    for (;;) {
        int size = vsnprintf(buffer.c_array(), buffer.size(), format, vargs);
        if (size > -1 && size < (int) buffer.size()) {
            // Guessed size was enough. Assign the string.
            message.assign (buffer.c_array(), size);
            break;
        }
        // Guessed size was not enough.
        if (size > -1) {
            // Reallocate a new buffer that will fit.
            buffer = array<char> (size + 1);
        } else {
            // Implementation of vsnprintf() did not return a valid required size.
            // Reallocate a new buffer. Double the guess of size.
            buffer = array<char> (buffer.size() * 2);
        }
    }
}

const char* TraceException::what() const throw()
{
    if( message.empty() )
        return exception::what() ;
    else
        return message.c_str() ;
}
