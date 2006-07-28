/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.xnet;

import java.util.Properties;


/**
 * The Server will call the following methods.
 * <p/>
 * newInstance()
 * init( port, properties)
 * start()
 * stop()
 * <p/>
 * All ServerService implementations must have a no argument
 * constructor.
 */
public interface ServerService extends SocketService {

    public void init(Properties props) throws Exception;

    public void start() throws ServiceException;

    public void stop() throws ServiceException;


    /**
     * Gets the ip number that the
     * daemon is listening on.
     */
    public String getIP();

    /**
     * Gets the port number that the
     * daemon is listening on.
     */
    public int getPort();


}
