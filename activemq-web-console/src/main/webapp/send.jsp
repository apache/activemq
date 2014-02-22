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
<c:set var="pageTitle" value="Send Messages"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>


<h2>Send a JMS Message</h2>

<form action="sendMessage.action" method="post">
<input type="hidden" name="secret" value="<c:out value='${sessionScope["secret"]}'/>"/>

<table id="headers" class="autostripe">
<thead>
<tr>
<th colspan="4">Message Header</th>
</tr>
</thead>

<tbody>
<tr>
	<td>
	    <label for="JMSDestination">Destination</label>
	</td>
	<td>
	    <input type="text" class="form-control" name="JMSDestination" defaultValue="foo.bar" />
	</td>
	<td>
	    <label for="queue">Queue or Topic</label>
	</td>
	<td>
	    <select class="form-control" name="JMSDestinationType">
	      <option name="JMSDestinationType" value="queue" label="Queue">Queue</option>
	      <option name="JMSDestinationType" value="topic" label="Topic">Topic</option>
	   </select>
	</td>
</tr>
<tr>
	<td>
	    <label for="JMSCorrelationID">Correlation ID</label>
	</td>
	<td>
	    <input type="text" name="JMSCorrelationID"  class="form-control" />
	</td>
	<td>
	    <label for="JMSPersistent">Persistent Delivery</label>
	</td>
	<td>
	    <input type="checkbox" name="JMSPersistent" />
	</td>
</tr>
<tr>
	<td>
	    <label for="JMSReplyTo">Reply To</label>
	</td>
	<td>
	    <input type="text" name="JMSReplyTo"  class="form-control" />
	</td>
	<td>
	    <label for="JMSPriority">Priority</label>
	</td>
	<td>
	    <input type="text" name="JMSPriority"  class="form-control" />
	</td>
</tr>
<tr>
	<td>
	    <label for="JMSType">Type</label>
	</td>
	<td>
	    <input type="text" name="JMSType"  class="form-control" />
	</td>
	<td>
	    <label for="JMSTimeToLive">Time to live</label>
	</td>
	<td>
	    <input type="text" name="JMSTimeToLive"  class="form-control"/>
	</td>
</tr>
<tr>
	<td>
	    <label for="JMSXGroupID">Message Group</label>
	</td>
	<td>
	    <input type="text" name="JMSXGroupID"  class="form-control"/>
	</td>
	<td>
	    <label for="JMSXGroupSeq">Message Group Sequence Number</label>
	</td>
	<td>
	    <input type="text" name="JMSXGroupSeq"  class="form-control"/>
	</td>
</tr>
<tr>
	<td>
	    <label for="AMQ_SCHEDULED_DELAY">delay(ms)</label>
	</td>
	<td>
	    <input type="text" name="AMQ_SCHEDULED_DELAY"  class="form-control"/>
	</td>
	<td>
	    <label for="AMQ_SCHEDULED_PERIOD">Time(ms) to wait before scheduling again</label>
	</td>
	<td>
	    <input type="text" name="AMQ_SCHEDULED_PERIOD"  class="form-control"/>
	</td>
</tr>
<tr>
	<td>
	    <label for="AMQ_SCHEDULED_REPEAT">Number of repeats</label>
	</td>
	<td>
	    <input type="text" name="AMQ_SCHEDULED_REPEAT"  class="form-control"/>
	</td>
	<td>
	    <label for="AMQ_SCHEDULED_CRON">Use a CRON string for scheduling</label>
	</td>
	<td>
	    <input type="text" name="AMQ_SCHEDULED_CRON"  class="form-control"/>
	</td>
</tr>
<tr>
	<td>
	    <label for="JMSMessageCount">Number of messages to send</label>
	</td>
	<td>
	    <input type="text" class="form-control" name="JMSMessageCount" defaultValue="1"/>
	</td>
	<td>
	    <label for="JMSMessageCountHeader">Header to store the counter</label>
	</td>
	<td>
	    <input type="text" class="form-control" name="JMSMessageCountHeader" defaultValue="JMSXMessageCounter"/>
	</td>
</tr>
<tr>
 <td colspan="4" align="center">
     <input type="submit" value="Send" class="btn" />
     <input type="reset" class="btn" />
 </td>
</tr>
<tr>
	<th colspan="4">
	    <label for="text">Message body</label>
	</th>
</tr>
<tr>
	<td colspan="4">
	    <textarea name="JMSText" rows="30" cols="80" class="form-control">Enter some text here for the message body...</textarea>
	</td>
</tr>
</tbody>
</table>

</form>
<%@include file="decorators/footer.jsp" %>

</body>
</html>
