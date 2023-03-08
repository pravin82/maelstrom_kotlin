#!/usr/bin/env kotlin


@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
@file:Import("dtos.main.kts")

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread

class Node(
    val nodeId:String,
    val nodeIds:List<String>,
    val nextMsgId:Int
) {
    private val lock = ReentrantLock()
    private val logLock = ReentrantLock()
    private val mapper = jacksonObjectMapper()
    private val messages = mutableListOf<Int?>()
    private val neighbors = mutableListOf<String?>()
    private val unackNeighborsMap = mutableMapOf<Int,MutableList<String?>>()


    fun logMsg(msg:String) {
        logLock.tryLock(5,TimeUnit.SECONDS)
        System.err.println(msg)
        System.out.flush()
        logLock.unlock()
    }


    fun sendReplyMsg(echoMsg: EchoMsg){
        val body = echoMsg.body
        val randMsgId = (0..10000).random()
        val replyType = body.type+"_ok"
        val replyBody =  when(body.type){
            "init" -> {
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
            "echo" -> {
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, echo = body.echo )
            }
            "read" -> {
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId , messages = messages)
            }
            "topology" -> {
                val nodeIds = body.topology?.get(nodeId)?: emptyList<String>()
                neighbors.addAll(nodeIds)
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
            "broadcast" -> {
                if(body.message !in messages ){
                    messages.add(body.message)
                    lock.tryLock(5,TimeUnit.SECONDS)
                    unackNeighborsMap.put(body.message?:-5, neighbors)
                    val unackNeighbors = unackNeighborsMap.get(body.message)
                    unackNeighbors?.remove(echoMsg.src)
                    thread{
                        while(unackNeighbors?.size?:0>0){
                            unackNeighbors?.map{neighborId->
                                sendMsg(neighborId?:"",echoMsg)
                                Thread.sleep(1000)
                            }
                        }

                    }
                    lock.unlock()


                }

                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
            "broadcast_ok" -> {
                System.err.println("BROAD_OK Init: $unackNeighborsMap")
                lock.tryLock(5, TimeUnit.SECONDS)
                val unackNeighbors = unackNeighborsMap.get(body.message)
                unackNeighbors?.remove(echoMsg.src)
                System.err.println("BROAD_OK After: $unackNeighborsMap")
                lock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId , messages = messages)
            }
            else -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
                System.err.println("In else ${body.type} message  recived")
                logLock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
        }

        val msg = EchoMsg(echoMsg.id,echoMsg.src,replyBody,echoMsg.dest)
        val replyStr =   mapper.writeValueAsString(msg)
        if(body.type == "broadcast_ok") return
        System.err.println("UnAckNO $unackNeighborsMap")
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()
        

    }
//MsgId will be -1 if it is being sent from node
    fun sendMsg(destId:String, msg:EchoMsg){
       val body = msg.body
       val bodyToBeSent = EchoBody(body.type,msgId = (0..10000).random(), inReplyTo = body.msgId, message = body.message)
       val msgToBeSent =  EchoMsg(msg.id,destId, bodyToBeSent, nodeId)
        val replyStr =   mapper.writeValueAsString(msgToBeSent)
        lock.tryLock(5,TimeUnit.SECONDS)
    System.err.println("Sent to Neighbor $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()
        
    }
}
