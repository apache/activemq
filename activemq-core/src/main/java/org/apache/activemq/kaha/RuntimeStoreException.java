/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.kaha;


/**
*Runtime exception for the Store
* 
* @version $Revision: 1.2 $
*/

public class RuntimeStoreException extends RuntimeException{
    
   
    private static final long serialVersionUID=8807084681372365173L;

    /**
     * Constructor
     */
    public RuntimeStoreException(){
        super();
    }

    /**
     * Constructor
     * @param message
     */
    public RuntimeStoreException(String message){
        super(message);
    }

    /**
     * Constructor
     * @param message
     * @param cause
     */
    public RuntimeStoreException(String message,Throwable cause){
        super(message,cause);
    }

    /**
     * Constructor
     * @param cause
     */
    public RuntimeStoreException(Throwable cause){
        super(cause);
    }
}
