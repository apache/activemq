// -----------------
// Original code by Joe Walnes
// -----------------

function chooseNickName() {
    var newNickName = prompt("Please choose a nick name", nickName)
    if (newNickName) {
        connection.sendMessage(chatTopic, "<message type='status'>" + nickName + " is now known as " + newNickName + "</message>")
        nickName = newNickName
    }
}

// when user clicks 'send', broadcast a message
function saySomething() {
    var text = document.getElementById("userInput").value
    connection.sendMessage(chatTopic, "<message type='chat' from='" + nickName + "'>" + text + "</message>")
    document.getElementById("userInput").value = ""
}

// when message is received from topic, display it in chat log
function receiveMessage(message) {
    var root = message.documentElement
    var chatLog = document.getElementById("chatLog")

    var type = root.getAttribute('type')
    if (type == 'status') {
        chatLog.value += "*** " + elementText(root) + "\n"
    }
    else if (type == 'chat') {
        chatLog.value += "<" + root.getAttribute('from') + "> " + elementText(root) + "\n"
    }
    else {
        chatLog.value += "*** Unknown type: " + type + " for: " + root + "\n"
    }
}

// returns the text of an XML element
function elementText(element) {
    var answer = ""
    var node = element.firstChild
    while (node != null) {
        var tmp = node.nodeValue
        if (tmp != null) {
            answer += tmp
        }
        node = node.nextSibling
    }
    return answer
}

var connection = new Connection("jms/FOO/BAR")
var chatTopic = "FOO.BAR"
connection.addMessageListener(chatTopic, receiveMessage)
var nickName = "unknown"
document.getElementById("chatLog").value = ''
