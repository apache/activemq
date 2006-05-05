<html>
<head>
<title>Test Pages</title>
</head>
<body>

<h2>Test Pages</h2>

These pages are used to test out the environment and web framework.

<table class="autostripe">
<thead>
<tr>
<th colspan="2">Headers</th>
</tr>
</thead>
<tbody>
<tr> 
  <td class="label">request.contextPath</td>
  <td>${request.contextPath}</td>
</tr>
<tr> 
  <td class="label">request.requestURI</td>
  <td>${request.requestURI}</td>
</tr>
<tr> 
  <td class="label">request.remoteAddr</td>
  <td>${request.remoteAddr}</td>
</tr>
<tr> 
  <td class="label">request.remoteHost</td>
  <td>${request.remoteHost}</td>
</tr>
<tr> 
  <td class="label">request.queryString</td>
  <td>${request.queryString}</td>
</tr>
<tr> 
  <td class="label">request.scheme</td>
  <td>${request.scheme}</td>
</tr>
<tr> 
  <td class="label">request.serverName</td>
  <td>${request.serverName}</td>
</tr>
<tr> 
  <td class="label">request.serverPort</td>
  <td>${request.serverPort}</td>
</tr>
<tr> 
  <td class="label">Spring applicationContext</td>
  <td>${applicationContext}</td>
</tr>
<tr> 
  <td class="label">Spring requestContext</td>
  <td>${requestContext}</td>
</tr>
<tr> 
  <td class="label">System properties</td>
  <td><%= System.getProperties() %></td>
</tr>
</tbody>
</table>


</body>
</html>
	