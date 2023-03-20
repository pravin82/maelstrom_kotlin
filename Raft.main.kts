#!/usr/bin/env kotlin
@file:Repository("https://jcenter.bintray.com")
@file:Import("dtos.main.kts")

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock





class StorageMap(){
    val storageMap = mutableMapOf<String,Int>()
    fun apply(op:EchoBody):OpResult{
        val key = op.key?:""

     val result =   when(op.type){
            "read" -> {
             val value = storageMap.get(key)
                if(value != null){
                    OpResult("read_ok", value)
                }
                else  OpResult("error",msg = "key not found")
            }
            "write" -> {
                storageMap.put(key, op.value?:-1)
                OpResult("write_ok")
            }
            "cas" -> {
                val value = storageMap.get(key)
                if(value != null) {
                    if(value != op.from) OpResult("error",msg = "expected ${op.from}, but had ${value}",code = 22)
                    else {
                        storageMap.put(key, op.to?:-1)
                        OpResult("cas_ok")
                    }

                }
                else  OpResult("error",msg = "key not found", code = 20)

            }
            else  -> {OpResult("error",msg =  "error msg")}
        }
        return result

    }
}

class Raft(){
    val stateMachine = StorageMap()
    var candidateState = "follower"
    val lock = ReentrantLock()
    val stateLock = ReentrantLock()
    fun handleClientReq(body:EchoBody):EchoBody{
        lock.tryLock(5,TimeUnit.SECONDS)
        val randMsgId = (0..10000).random()
       val opResult =  stateMachine.apply(body)
        lock.unlock()
        val  replyBody =  EchoBody(opResult.type,msgId = randMsgId, inReplyTo = body.msgId, value = opResult.value )
        return replyBody
    }
    fun becomeCandidate(){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        candidateState = "candidate"
        System.err.println("Became candidate")
        stateLock.unlock()

    }
    
}