/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activeio.oneport.openorb;

import org.omg.PortableInterceptor.ORBInitInfo;
import org.openorb.orb.pi.FeatureInitInfo;
import org.openorb.orb.pi.FeatureInitializer;

/**
 * Used to hook in the OpenORBOpenPortSocketFactory into the ORB.
 */
public class OpenORBOpenPortFeatureInitializer implements FeatureInitializer {
    
    static final private ThreadLocal socketFatory = new ThreadLocal();
    
    static public void setContextSocketFactory( OpenORBOpenPortSocketFactory sf ) {
        socketFatory.set(sf);
    }
    
    public void init(ORBInitInfo orbinfo, FeatureInitInfo featureinfo) {
        OpenORBOpenPortSocketFactory sf = (OpenORBOpenPortSocketFactory) socketFatory.get();
        if( sf!=null ) {
            featureinfo.setFeature("IIOP.SocketFactory", sf);                    
        }
    }
}
