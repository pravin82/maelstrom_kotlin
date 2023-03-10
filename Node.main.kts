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
    private val logLock = ReentrantLock()
    private val mapper = jacksonObjectMapper()
    private val crdt = GCounter(mutableMapOf<String,Int>(), nodeId)
    private val neighbors = mutableListOf<String?>()
    private val unackNeighborsMap = mutableMapOf<Int,MutableList<String?>>()
    private val valueStore = mutableMapOf<Int, MutableList<Int>>()


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
                val txns = executeTxns(body.txn?:emptyList())
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId,txn = txns)
            }
            

            "add" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                crdt.addElement(body.delta?:0)
                lock.unlock()
                EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, echo = body.echo )

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
        if(body.type in listOf( "broadcast_ok", "replicate")) return
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()
        

    }
//MsgId will be -1 if it is being sent from node
    fun sendMsg(destId:String, msg:EchoMsg){
       val body = msg.body
       val bodyToBeSent = EchoBody(body.type,msgId = (0..10000).random(), inReplyTo = body.msgId,counterMap = body.counterMap)
       val msgToBeSent =  EchoMsg(msg.id,destId, bodyToBeSent, nodeId)
        val replyStr =   mapper.writeValueAsString(msgToBeSent)
       System.err.println("Sent to Neighbor $replyStr")
        System.out.println( replyStr)
        System.out.flush()

    }
    fun replicateMsgScheduler(){
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                replicateMsg()
            }
        }, 0, 5000)

    }


    fun replicateMsg(){
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Replicate Called CRDT7: ${crdt.counterMap}")
        nodeIds.map{
            val bodyToBeSent = EchoBody("replicate",msgId = 0,counterMap = crdt.counterMap )
            val msgToBeSent =  EchoMsg(1,it, bodyToBeSent, nodeId)
            sendMsg(it,msgToBeSent)
        }
        lock.unlock()
    }


    fun executeTxns(txns:List<List<Any?>>):List<List<Any?>>{
      val completedTxns =   txns.map{txn->
            val txnType = txn.first()
            val txnKey = txn[1] as Int
            val txnValue = txn[2]
           val valueStored = valueStore.get(txnKey) as List<Int>?
            when(txnType){
             "r" -> {
                 listOf(txnType, txnKey, valueStored)
             }
                "append" -> {
                    val modifiedValue = ((valueStored?:emptyList<Int>()).plus(txnValue)).toMutableList() as MutableList<Int>
                    valueStore.put(txnKey,modifiedValue )
                    listOf(txnType, txnKey, txnValue)

                }
                else ->  listOf(txnType, txnKey, txnValue)

            }
            
        }
        return completedTxns
    }
}
