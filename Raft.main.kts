#!/usr/bin/env kotlin

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

data class Operation (
    val type:String,
    val msgId:Int,
    val key:String,
    val value:String?,
    val from:String?,
    val to:String?
        )

data class OpResult(
    val type:String,
    val value:String
)

data class Msg(
    val body:Operation
)

class StorageMap(){
    val storageMap = mutableMapOf<String,String>()
    fun apply(op:Operation):OpResult{
        val key = op.key

     val result =   when(op.type){
            "read" -> {
             val value = storageMap.get(key)
                if(value != null){
                    OpResult("read_ok", value)
                }
                else  OpResult("error","key not found")
            }
            "write" -> {
                storageMap.put(key, op.value?:"")
                OpResult("write_ok", op.value?:"")
            }
            "cas" -> {
                val value = storageMap.get(key)
                if(value != null) {
                    if(value != op.from) OpResult("error","expected ${op.from}, but had ${value}")
                    else {
                        storageMap.put(key, op.to?:"")
                        OpResult("cas_ok",op.to?:"")
                    }

                }
                else  OpResult("error","key not found")

            }
            else  -> {OpResult("error", "error msg")}
        }
        return result

    }
}

class Raft(){
    val stateMachine = StorageMap()
    val lock = ReentrantLock()
    fun handleClientReq(msg:Msg){
        lock.tryLock(5,TimeUnit.SECONDS)
       val resp =  stateMachine.apply(msg.body)
        lock.unlock()

    }
}