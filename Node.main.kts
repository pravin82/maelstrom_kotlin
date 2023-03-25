#!/usr/bin/env kotlin
@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
@file:Import("dtos.main.kts","Raft.main.kts")

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread

class Node(
    val nodeId:String,
    val nextMsgId:Int
) {
    
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private val writeCondition = lock.newCondition()
    private val logLock = ReentrantLock()
    private val mapper = jacksonObjectMapper()
    private var nodeIds:List<String> = emptyList()
    private val neighbors = mutableListOf<String?>()
    private val unackNeighborsMap = mutableMapOf<Int,MutableList<String?>>()
    private var readValue = emptyList<Int>()
    private var doesReadValueRec = false
    private var doesWriteValueRec = false
    private val databaseKey = "ROOT"
    private var databaseValue = emptyMap<Int, List<Int>>()
     var raft = Raft(nodeId,emptyList())




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
                nodeIds = body.nodeIds?:emptyList()
                raft = Raft(nodeId,nodeIds)
               thread{
                   raft.candidateScheduler()
                   raft.stepDownScheduler()
               }

                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
            "read","write","cas", "request_vote", "request_vote_res" -> {
            val raftBody =   raft.handleClientReq(echoMsg)
                raftBody

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
        if(body.type in listOf( "broadcast_ok", "replicate","read_ok","cas_ok","error","request_vote","request_vote_res", "txn","request_vote_ok", "request_vote_res_ok" )) return
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()

    }

//MsgId will be -1 if it is being sent from node
    fun sendMsg(destId:String, body:EchoBody):Map<Int,List<Int>>{
       var list = emptyList<Int>()
       lock.tryLock(5,TimeUnit.SECONDS)
       val msgToBeSent =  EchoMsg(1,destId, body, nodeId)
       val replyStr =   mapper.writeValueAsString(msgToBeSent)
      val thread1 = Thread.currentThread()
       System.err.println("[ThreadN:${thread1.name}]Sent to tin-kv $replyStr")
        System.err.println("[ThreadN:${thread1.name}] doesReadValueRec: ${doesReadValueRec}")
        System.out.println( replyStr)
       System.out.flush()
      doesReadValueRec = false
       while(!doesReadValueRec){
           condition.await()
       }
       lock.unlock()
       return databaseValue

    }




}
