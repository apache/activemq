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
 
#ifndef _CMS_BYTESMESSAGE_H_
#define _CMS_BYTESMESSAGE_H_
 
#include <cms/Message.h>

namespace cms{
   
    class BytesMessage : public Message{
      
    public:
   
        virtual ~BytesMessage(){}

        /**
         * sets the bytes given to the message body.  
         * @param Byte Buffer to copy
         * @param Number of bytes in Buffer to copy
         * @throws CMSException
         */
        virtual void setBodyBytes( 
            const unsigned char* buffer, const unsigned long numBytes ) 
                throw( CMSException ) = 0;
            
        /**
         * Gets the bytes that are contained in this message, user should
         * copy this data into a user allocated buffer.  Call 
         * <code>getBodyLength</code> to determine the number of bytes
         * to expect.
         * @return const pointer to a byte buffer
         */
        virtual const unsigned char* getBodyBytes(void) const = 0;
      
        /**
         * Returns the number of bytes contained in the body of this message.
         * @return number of bytes.
         */
        virtual unsigned long getBodyLength(void) const = 0;
      
   };
}

#endif /*_CMS_BYTESMESSAGE_H_*/
