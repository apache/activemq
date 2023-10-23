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
<c:set var="pageTitle" value="Producers for Topic ${requestContext.topicProducerQuery.JMSDestination}"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>



<h2>Active Producers for <c:out value="${requestContext.topicProducerQuery.JMSDestination}" /></h2>

<table id="producers" class="sortable autostripe">
<thead>
<tr>
	<th>
		<span>Client ID</span>
		<br/>
		<span>Connection ID</span>
	</th>
	<th>SessionId</th>
	<th>ProducerId</th>
	<th>ProducerWindowSize</th>
	<th>DispatchAsync</th>
	<th>Blocked</th>
	<th>BlockedTime</th>
	<th>SentCount</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.topicProducerQuery.producers}" var="row">
<tr>
	<td>
        <a href="<c:out value="connection.jsp?connectionID=${row.clientId}"/>"><c:out value="${row.clientId}" /></a><br/>
        <br>
        <c:out value="${row.connectionId}" />
    </td>
	<td><c:out value="${row.sessionId}" /></td>
	<td><c:out value="${row.producerId}" /></td>
	<td><c:out value="${row.producerWindowSize}" /></td>
	<td><c:out value="${row.dispatchAsync}" /></td>
	<td><c:out value="${row.producerBlocked}" /></td>
	<td><c:out value="${row.totalTimeBlocked}" /></td>
	<td><c:out value="${row.sentCount}" /></td>
</tr>
</c:forEach>
</tbody>
</table>
<%@include file="decorators/footer.jsp" %>

</body>
</html>
