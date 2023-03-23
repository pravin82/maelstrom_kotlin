#!/usr/bin/env kotlin
//./maelstrom test -w lin-kv --bin /Users/pravin/script2/script2.main.kts --time-limit 10 --rate 10 --node-count 1 --concurrency 2n --log-stderr

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
@file:Import("Node.main.kts","dtos.main.kts")

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

val mapper = jacksonObjectMapper()
val EMPTY_STRING = ""
var nextMsgId = 12



val nodeMap = mutableMapOf<String,Node>()
while(true){
     val input = readLine()
     val echoMsg = mapper.readValue(input, EchoMsg::class.java)
      val body = echoMsg.body
      if(body.type == "init"){
          val newNode = Node(echoMsg.dest, 0)
          newNode.raft.candidateScheduler()
          nodeMap.put(echoMsg.dest, newNode)
      }
    val thread1 = Thread.currentThread()
   val node =  nodeMap.get(echoMsg.dest)
    System.err.println("[ThreadName1:${thread1.name}]Received $input")
    node?.sendReplyMsg(echoMsg)

}











