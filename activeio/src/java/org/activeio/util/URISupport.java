/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.activeio.util;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * The URISupport class provides a few static methods that provides a few usefull
 * operations to manipulate URIs.
 * 
 * @version $Revision$
 */
public class URISupport {
    
    static public URI changePort(URI bindAddr, int port) throws URISyntaxException {
        return new URI(bindAddr.getScheme(), bindAddr.getUserInfo(), bindAddr.getHost(), port, bindAddr.getPath(), bindAddr.getQuery(), bindAddr.getFragment());
    }
    static public URI changeScheme(URI bindAddr, String scheme) throws URISyntaxException {
        return new URI(scheme, bindAddr.getUserInfo(), bindAddr.getHost(), bindAddr.getPort(), bindAddr.getPath(), bindAddr.getQuery(), bindAddr.getFragment());
    }
    static public URI changeUserInfo(URI bindAddr, String userInfo) throws URISyntaxException {
        return new URI(bindAddr.getScheme(), userInfo, bindAddr.getHost(), bindAddr.getPort(), bindAddr.getPath(), bindAddr.getQuery(), bindAddr.getFragment());
    }
    static public URI changeHost(URI bindAddr, String host) throws URISyntaxException {
        return new URI(bindAddr.getScheme(), bindAddr.getUserInfo(), host, bindAddr.getPort(), bindAddr.getPath(), bindAddr.getQuery(), bindAddr.getFragment());
    }
    static public URI changePath(URI bindAddr, String path) throws URISyntaxException {
        return new URI(bindAddr.getScheme(), bindAddr.getUserInfo(), bindAddr.getHost(), bindAddr.getPort(), path, bindAddr.getQuery(), bindAddr.getFragment());
    }
    static public URI changeQuery(URI bindAddr, String query) throws URISyntaxException {
        return new URI(bindAddr.getScheme(), bindAddr.getUserInfo(), bindAddr.getHost(), bindAddr.getPort(), bindAddr.getPath(), query, bindAddr.getFragment());
    }
    static public URI changeFragment(URI bindAddr, String fragment) throws URISyntaxException {
        return new URI(bindAddr.getScheme(), bindAddr.getUserInfo(), bindAddr.getHost(), bindAddr.getPort(), bindAddr.getPath(), bindAddr.getQuery(), fragment);
    }

}
