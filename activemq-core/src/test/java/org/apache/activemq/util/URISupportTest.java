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
package org.apache.activemq.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import junit.framework.TestCase;
import org.apache.activemq.util.URISupport.CompositeData;

/**
 *
 * 
 */
public class URISupportTest extends TestCase {
    
    public void testEmptyCompositePath() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("broker:()/localhost?persistent=false"));
        assertEquals(0, data.getComponents().length);        
    }
            
    public void testCompositePath() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:(path)/path"));
        assertEquals("path", data.getPath());        
        data = URISupport.parseComposite(new URI("test:path"));
        assertNull(data.getPath());
    }

    public void testSimpleComposite() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:part1"));
        assertEquals(1, data.getComponents().length);
    }

    public void testComposite() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:(part1://host,part2://(sub1://part,sube2:part))"));
        assertEquals(2, data.getComponents().length);
    }

    
    public void testCompositeWithComponentParam() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:(part1://host?part1=true)?outside=true"));
        assertEquals(1, data.getComponents().length);
        assertEquals(1, data.getParameters().size());
        Map part1Params = URISupport.parseParameters(data.getComponents()[0]);
        assertEquals(1, part1Params.size());
        assertTrue(part1Params.containsKey("part1"));
    }
    
    public void testParsingURI() throws Exception {
        URI source = new URI("tcp://localhost:61626/foo/bar?cheese=Edam&x=123");
        
        Map map = URISupport.parseParameters(source);
    
        assertEquals("Size: " + map, 2, map.size());
        assertMapKey(map, "cheese", "Edam");
        assertMapKey(map, "x", "123");
        
        URI result = URISupport.removeQuery(source);
        
        assertEquals("result", new URI("tcp://localhost:61626/foo/bar"), result);
    }
    
    protected void assertMapKey(Map map, String key, Object expected) {
        assertEquals("Map key: " + key, map.get(key), expected);
    }
    
    public void testParsingCompositeURI() throws URISyntaxException {
        CompositeData data = URISupport.parseComposite(new URI("broker://(tcp://localhost:61616)?name=foo"));
        assertEquals("one component", 1, data.getComponents().length);
        assertEquals("Size: " + data.getParameters(), 1, data.getParameters().size());
    }
    
    public void testCheckParenthesis() throws Exception {
        String str = "fred:(((ddd))";
        assertFalse(URISupport.checkParenthesis(str));
        str += ")";
        assertTrue(URISupport.checkParenthesis(str));
    }
    
    public void testCreateWithQuery() throws Exception {
        URI source = new URI("vm://localhost");
        URI dest = URISupport.createURIWithQuery(source, "network=true&one=two");
        
        assertEquals("correct param count", 2, URISupport.parseParameters(dest).size());
        assertEquals("same uri, host", source.getHost(), dest.getHost());
        assertEquals("same uri, scheme", source.getScheme(), dest.getScheme());
        assertFalse("same uri, ssp", dest.getQuery().equals(source.getQuery()));
    }
    
    public void testParsingParams() throws Exception {
        URI uri = new URI("static:(http://localhost:61617?proxyHost=localhost&proxyPort=80)");
        Map<String,String>parameters = URISupport.parseParameters(uri);
        verifyParams(parameters);
        uri = new URI("static://http://localhost:61617?proxyHost=localhost&proxyPort=80");
        parameters = URISupport.parseParameters(uri);
        verifyParams(parameters);
        uri = new URI("http://0.0.0.0:61616");
        parameters = URISupport.parseParameters(uri);
    }
    
    public void testCompositeCreateURIWithQuery() throws Exception {
    	String queryString = "query=value";
    	URI originalURI = new URI("outerscheme:(innerscheme:innerssp)");
    	URI querylessURI = originalURI;
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, null));
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, ""));
    	assertEquals(new URI(querylessURI + "?" + queryString), URISupport.createURIWithQuery(originalURI, queryString));
    	originalURI = new URI("outerscheme:(innerscheme:innerssp)?outerquery=0");
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, null));
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, ""));
    	assertEquals(new URI(querylessURI + "?" + queryString), URISupport.createURIWithQuery(originalURI, queryString));
    	originalURI = new URI("outerscheme:(innerscheme:innerssp?innerquery=0)");
    	querylessURI = originalURI;
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, null));
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, ""));
    	assertEquals(new URI(querylessURI + "?" + queryString), URISupport.createURIWithQuery(originalURI, queryString));
    	originalURI = new URI("outerscheme:(innerscheme:innerssp?innerquery=0)?outerquery=0");
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, null));
    	assertEquals(querylessURI, URISupport.createURIWithQuery(originalURI, ""));
    	assertEquals(new URI(querylessURI + "?" + queryString), URISupport.createURIWithQuery(originalURI, queryString));
    }
    
    private void verifyParams(Map<String,String> parameters) {
        assertEquals(parameters.get("proxyHost"), "localhost");
        assertEquals(parameters.get("proxyPort"), "80");
    }
    
}
