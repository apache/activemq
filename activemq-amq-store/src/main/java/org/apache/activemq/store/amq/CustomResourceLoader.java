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
package org.apache.activemq.store.amq;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.resource.Resource;
import org.apache.velocity.runtime.resource.loader.FileResourceLoader;
import org.apache.velocity.runtime.resource.loader.ResourceLoader;

public class CustomResourceLoader extends ResourceLoader {
	
	private final static ThreadLocal<HashMap<String, String>> resourcesTL = new ThreadLocal<HashMap<String, String>>();
	private final FileResourceLoader fileResourceLoader = new FileResourceLoader();
	
	@Override
	public void commonInit(RuntimeServices rs, ExtendedProperties configuration) {
		super.commonInit(rs, configuration);
		fileResourceLoader.commonInit(rs, configuration);
	}
	
    public void init( ExtendedProperties configuration)
    {
    	fileResourceLoader.init(configuration);
    }
    
    /**
     */
    public synchronized InputStream getResourceStream( String name )
        throws ResourceNotFoundException
    {
        InputStream result = null;
        
        if (name == null || name.length() == 0)
        {
            throw new ResourceNotFoundException ("No template name provided");
        }
        
        String value = null;
        HashMap<String, String> resources = resourcesTL.get();
        if( resources!=null ) {
        	value = resources.get(name);
        }
        
    	if( value == null ) {
    		result = this.fileResourceLoader.getResourceStream(name);
    	} else {
            try 
            {
            	result = new ByteArrayInputStream(value.getBytes());
            }
            catch( Exception e )
            {
                throw new ResourceNotFoundException( e.getMessage() );
            }
    	}
        return result;
    }
    
    public boolean isSourceModified(Resource resource)
    {
        return false;
    }

    public long getLastModified(Resource resource)
    {
        return 0;
    }

	static public HashMap<String, String> getResources() {
		return resourcesTL.get();
	}

	static public void setResources(HashMap<String, String> arg0) {
		resourcesTL.set(arg0);
	}

}
