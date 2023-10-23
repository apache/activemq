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
<c:set var="pageTitle" value="Network Bridges"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>

<div style="margin-top: 5em">
<h2>Network Bridges</h2>

<table id="bridges" class="sortable autostripe">
<thead>
<tr>
    <th>Remote Broker</th>
    <th>Remote Address</th>
    <th>Created By Duplex</th>
    <th>Messages Enqueued</th>
    <th>Messages Dequeued</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.networkBridges}" var="nb">
<tr>
	<td><c:out value="${nb.remoteBrokerName}" /></td>
	<td><c:out value="${nb.remoteAddress}" /></td>
	<td><c:out value="${nb.createdByDuplex}" /></td>
	<td><c:out value="${nb.enqueueCounter}" /></td>
	<td><c:out value="${nb.dequeueCounter}" /></td>
</tr>
</c:forEach>
</tbody>
</table>
</div>
<%@include file="decorators/footer.jsp" %>

</body>
</html>

