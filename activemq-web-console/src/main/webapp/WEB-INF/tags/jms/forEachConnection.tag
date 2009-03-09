<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
   
    http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<%@ attribute name="connection" type="java.lang.String" required="true"  %>
<%@ attribute name="connectionName" type="java.lang.String" required="true"  %>
<%@ attribute name="broker" type="org.apache.activemq.web.BrokerFacade" required="true"  %>
<%@ attribute name="connectorName" type="java.lang.String" required="true"  %>
<%@ tag import="java.util.Iterator" %>
<%@ tag import="org.apache.activemq.broker.jmx.ConnectionViewMBean" %>
<%
	Iterator it = broker.getConnections(connectorName).iterator();
	while (it.hasNext()) {
		String conName = (String) it.next();
		ConnectionViewMBean con = broker.getConnection(conName);
		request.setAttribute(connectionName, conName);
		request.setAttribute(connection, con);
%>
<jsp:doBody/>
<%
	}
%>       
    
