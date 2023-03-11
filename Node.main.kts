#!/usr/bin/env kotlin
@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
@file:Import("dtos.main.kts","Gset.main.kts","GCounter.main.kts")

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread

class Node(
    val nodeId:String,
    val nodeIds:List<String>,
    val nextMsgId:Int
) {

    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private val writeCondition = lock.newCondition()
    private val logLock = ReentrantLock()
    private val mapper = jacksonObjectMapper()
    private val crdt = GCounter(mutableMapOf<String,Int>(), nodeId)
    private val neighbors = mutableListOf<String?>()
    private val unackNeighborsMap = mutableMapOf<Int,MutableList<String?>>()
    private var readValue = emptyList<Int>()
    private var doesReadValueRec = false
    private var doesWriteValueRec = false
    private val databaseKey = "ROOT"
    private var databaseValue = emptyMap<Int, List<Int>>()


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

            "txn" ->{
               thread{
                  val ops = executeOps(body.txn?:emptyList())
                   val body = EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId,txn = ops)
                   val msg = EchoMsg(echoMsg.id,echoMsg.src,body,echoMsg.dest)
                   val replyStr =   mapper.writeValueAsString(msg)
                   System.err.println("Sent Inside thread $replyStr")
                   System.out.println( replyStr)
                   System.out.flush()
               }
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId)

            }
            "error" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                condition.signal()
                lock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId)
            }

            "read_ok" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                 val databaseMap = body.value as   Map<Int,List<Int>>
                // val databaseMap = mapper.readValue(input, Map<Int,List<Int>>::class.java)
                 databaseValue = databaseMap
                doesReadValueRec = true
                condition.signal()
                lock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId)

            }
            

            "add" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                crdt.addElement(body.delta?:0)
                lock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, echo = body.echo )

            }
            "cas_ok" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesWriteValueRec = true
                writeCondition.signal()
                lock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId)

            }

            "topology" -> {
                val nodeIds = body.topology?.get(nodeId)?: emptyList<String>()
                neighbors.addAll(nodeIds)
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
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
        if(body.type in listOf( "broadcast_ok", "replicate","read_ok","cas_ok","error", "txn")) return
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

    fun sendMsg1(destId:String, body:EchoBody){
        lock.tryLock(5,TimeUnit.SECONDS)
        var list = emptyList<Int>()

        val msgToBeSent =  EchoMsg(1,destId, body, nodeId)
        val replyStr =   mapper.writeValueAsString(msgToBeSent)
        System.err.println("Writing to tin-kv $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        doesWriteValueRec = false
        while(!doesWriteValueRec){
            writeCondition.await()
        }
        lock.unlock()

    }


    fun executeOps(ops:List<List<Any?>>):List<List<Any?>>{
        val readReq =   EchoBody("read", key = databaseKey,msgId = (0..10000).random() )
        val databaseMap =   sendMsg("lin-kv",readReq )
        val databaseMapCopy = databaseMap.toMutableMap()
        var doesHaveAppendOp = false;
      val completedOps =   ops.map{op->
            val opType = op.first()
            val opKey = op[1] as Int
            val opValue = op[2]
            when(opType){
             "r" -> {
                 val value = databaseMapCopy.get(opKey) as List<Int>?
                 listOf(opType, opKey, value)
               }
                "append" -> {
                    doesHaveAppendOp = true
                    val value = databaseMapCopy.get(opKey) as List<Int>?
                    System.err.println("Append key:${opKey} value:${value}")
                    val modifiedList = (value?:emptyList<Int>()).toMutableList().plus(opValue as Int)
                    System.err.println("Append key:${opKey} value:${modifiedList}")
                    databaseMapCopy.put(opKey, modifiedList)

                    listOf(opType, opKey, opValue)

                }
                else ->  listOf(opType, opKey, opValue)

            }
        }
        if(doesHaveAppendOp){
            val writeReq = EchoBody("cas", key = databaseKey, from = databaseMap, to = databaseMapCopy, createIfNotExists = true ,msgId = (0..10000).random() )
            val appendValue = sendMsg1("lin-kv", writeReq)
        }

        return completedOps
    }
}
