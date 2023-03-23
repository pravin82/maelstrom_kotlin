#!/usr/bin/env kotlin

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
@file:Import("dtos.main.kts")

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.concurrent.thread








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

class Raft(val nodeIds:List<Int>){
    val stateMachine = StorageMap()
    var candidateState = "follower"
    var electionDeadline = System.currentTimeMillis()
    val electionTimeout = 2000
    var term = 0
    val lock = ReentrantLock()
    val stateLock = ReentrantLock()
    private val condition = lock.newCondition()
    val entriesLog = listOf(LogEntry(0)).toMutableList()
    val mapper = jacksonObjectMapper()
    var doesReadValueRec = true
    fun handleClientReq(body:EchoBody):EchoBody{
        when(body.type){
            "request_vote_res" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                condition.signal()
                lock.unlock()
                
            }
        }
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
        advanceTerm(term+1)
        resetElectionDeadline()
        System.err.println("Became candidate for term :${term}")
        stateLock.unlock()

    }

    fun becomeFollower(){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        candidateState = "follower"
        resetElectionDeadline()
        System.err.println("Became follower for term :${term}")
        stateLock.unlock()

    }

    fun sendVoteReq(){
        thread{
            nodeIds.map{
                val msg = VoteReqMsg("request_vote", term, it,entriesLog.size,entriesLog.last().term )
                val msgStr = mapper.writeValueAsString(msg)
                System.err.println("Vote Req Sent: ${msgStr}")
                sendSyncMsg(msgStr)

            }
        }

    }

    fun sendSyncMsg(msg:String){
        lock.tryLock(5,TimeUnit.SECONDS)
        System.out.println( msg)
        System.out.flush()
        doesReadValueRec = false
        while(!doesReadValueRec){
            condition.await()
        }
        lock.unlock()

    }

    fun maybeStepDown(remoteTerm:Int){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        if(term < remoteTerm){
            System.err.println("Stepping Down: remoteTerm: ${remoteTerm} Node term: ${term}")
            advanceTerm(remoteTerm)
            becomeFollower()
        }


    }

    fun candidateScheduler(){
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                if(electionDeadline < System.currentTimeMillis())
                if(candidateState != "leader" ){
                    becomeCandidate()
                } else {
                    resetElectionDeadline()
                }

            }
        }, 0, 2000)

    }

    fun resetElectionDeadline(){
        lock.tryLock(5,TimeUnit.SECONDS)
        electionDeadline = System.currentTimeMillis() +  (electionTimeout*(Math.random()+1)).toLong()
        lock.unlock()
    }

    fun advanceTerm(newTerm:Int){
        if(term > newTerm){
            System.err.println("Term can't go backwards")
        }
        else {
            term = newTerm
        }

    }
    
}