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
    private val valueStore = mutableMapOf<Int, MutableList<Int>>()
    private var readValue = emptyList<Int>()
    private var doesReadValueRec = false
    private var doesWriteValueRec = false
//  override   fun run(){
//       val currThread = Thread.currentThread()
//       System.err.println("Running on thread ${currThread.name}")
//   }

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
                  val txns = executeTxns(body.txn?:emptyList())
                   val body = EchoBody(replyType,msgId = randMsgId, inReplyTo = body.msgId,txn = txns)
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
             val value = body.value  as List<Int>
                readValue = value
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
    fun sendMsg(destId:String, body:EchoBody):List<Int>{
       var list = emptyList<Int>()
       lock.tryLock(5,TimeUnit.SECONDS)
       val msgToBeSent =  EchoMsg(1,destId, body, nodeId)
       val replyStr =   mapper.writeValueAsString(msgToBeSent)
      val thread1 = Thread.currentThread()
       System.err.println("[ThreadN:${thread1.name}]Sent to Neighbor $replyStr")
        System.err.println("[ThreadN:${thread1.name}] doesReadValueRec: ${doesReadValueRec}")
       System.out.println( replyStr)
       System.out.flush()
      doesReadValueRec = false
       while(!doesReadValueRec){
           condition.await()
       }
       list = readValue
       readValue = emptyList()
       lock.unlock()
       return list

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



    fun executeTxns(txns:List<List<Any?>>):List<List<Any?>>{
      val completedTxns =   txns.map{txn->
            val txnType = txn.first()
            val txnKey = txn[1] as Int
            val txnValue = txn[2]
             val readReq =   EchoBody("read", key = txnKey,msgId = (0..10000).random() )
           val valueStored = valueStore.get(txnKey) as List<Int>?
            when(txnType){
             "r" -> {

               val list =   sendMsg("lin-kv",readReq )
                 listOf(txnType, txnKey, list)
             }
                "append" -> {
                    val list  = sendMsg("lin-kv", readReq)
                    val modifiedList = list.plus(txnValue as Int)
                    val writeReq = EchoBody("cas", key = txnKey, from = list, to = modifiedList, createIfNotExists = true ,msgId = (0..10000).random() )
                    val appendValue = sendMsg1("lin-kv", writeReq)
                    listOf(txnType, txnKey, txnValue)

                }
                else ->  listOf(txnType, txnKey, txnValue)

            }
            
        }
        return completedTxns
    }
}
