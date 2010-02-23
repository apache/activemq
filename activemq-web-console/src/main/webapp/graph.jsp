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
<html>
<head>
<title>Browse ${requestContext.queueBrowser.JMSDestination}</title>
</head>
<body>

<h2>Browse ${requestContext.queueBrowser.JMSDestination}</h2>

<table id="messages" class="sortable autostripe">
<thead>
<tr>
<th>Message ID</th>
<th>Correlation ID</th>
<th>Persistence</th>
<th>Priority</th>
<th>Redelivered</th>
<th>Reply To</th>
<th>Timestamp</th>
<th>Type</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<%--    
<c:forEach items="${requestContext.queueBrowser.browser.enumeration}" var="row">
---%>
<jms:forEachMessage queueBrowser="${requestContext.queueBrowser.browser}" var="row">
<tr>
<td><a href="message.jsp?id=${row.JMSMessageID}" title="${row.JMSDestination}">${row.JMSMessageID}</a></td>
<td>${row.JMSCorrelationID}</td>
<td><jms:persistent message="${row}"/></td>
<td>${row.JMSPriority}</td>
<td>${row.JMSRedelivered}</td>
<td>${row.JMSReplyTo}</td>
<td>${row.JMSTimestamp}</td>
<td>${row.JMSType}</td>
<td>
    <a href="deleteDestination.action?destination=${row.JMSMessageID}&secret=<c:out value='${sessionScope["secret"]}'/>">Delete</a>
</td>
</tr>
</jms:forEachMessage>
<%--    
</c:forEach>
--%>
</tbody>
</table>


</body>
</html>
	
