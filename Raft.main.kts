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

class Raft(val nodeId:String,val nodeIds:List<String>){
    val stateMachine = StorageMap()
    var candidateState = "follower"
    var electionDeadline = System.currentTimeMillis()
    val electionTimeout = 2000
    var term = 0
    val lock = ReentrantLock()
    val neighBorIds = nodeIds.minus(nodeId)
    var voteResp = EchoMsg(0,"", EchoBody(""),"")
    val stateLock = ReentrantLock()
    private val condition = lock.newCondition()
    val entriesLog = listOf(LogEntry(0)).toMutableList()
    val mapper = jacksonObjectMapper()
    var doesReadValueRec = true
    var votedFor:String? = null
    val votes = mutableSetOf<String> (nodeId)
    fun handleClientReq(msg:EchoMsg):EchoBody{
        val body = msg.body
        when(body.type){
            "request_vote_res" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                voteResp = msg
                condition.signal()
                lock.unlock()
                
            }
            "request_vote" -> {
                respondToVoteRequest(body)
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
        lock.tryLock(5,TimeUnit.SECONDS)
        candidateState = "candidate"
        advanceTerm(term+1)
        resetElectionDeadline()
        System.err.println("Became candidate for term :${term}")
        votedFor = nodeId
        sendVoteReq()
        lock.unlock()

    }


    fun respondToVoteRequest(body:EchoBody){
        var grantVote = false
        maybeStepDown(body.term?:0)
        if(body.term?:0 < term) {
            System.err.println("Candidate term ${body.term} lower than ${term}, not granting vote.")
        }
        else if(votedFor != null){
            System.err.println("Already voted for ${votedFor}, not granting vote")
        }
        else if(body.lastLogTerm?:0 < entriesLog.last().term){
            System.err.println("Have log entries from term ${entriesLog.last().term} which is newer than remote term  ${body.lastLogTerm}, not granting vote")
        }
        else if(body.lastLogTerm == entriesLog.last().term && body.lastLogIndex?:0 < entriesLog.size){
            System.err.println("Our logs are both at term ${body.lastLogTerm} but our log is ${entriesLog.size} and their is body.lastLogIndex long, not granting vote")
        }
        else {
            System.err.println("Granting vote to ${body.candidateId}")
            grantVote = true
            votedFor = body.candidateId
            resetElectionDeadline()
        }
        lock.tryLock(5,TimeUnit.SECONDS)
        val replyBody = EchoBody("request_vote_res", term = term,voteGranted = grantVote )
        val randMsgId = (0..10000).random()
        val msg= EchoMsg(randMsgId,body.candidateId?:"", replyBody, nodeId )
        val msgStr = mapper.writeValueAsString(msg)
        System.err.println("Vote Request Resp sent : ${msgStr}")
        System.out.println( msgStr)
        System.out.flush()
        lock.unlock()




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
            neighBorIds.map{
                val randMsgId = (0..10000).random()
                val termForVoteRequested = term
                val replyBody = EchoBody(type = "request_vote", term = termForVoteRequested, candidateId = nodeId,lastLogIndex = entriesLog.size,lastLogTerm = entriesLog.last().term )
                val msg = EchoMsg(randMsgId, it, replyBody, nodeId)
                val msgStr = mapper.writeValueAsString(msg)
               // stateLock.tryLock(5,TimeUnit.SECONDS)
                System.err.println("Vote Req Msg Sent: ${msgStr}")
               // stateLock.unlock()
              val msgResp =   sendSyncMsg(msgStr)
                val body = msgResp.body
                val respTerm = body.term
                val voteGranted = body.voteGranted?:false
                maybeStepDown(respTerm?:0)
                if(candidateState == "candidate" && term == respTerm && term == termForVoteRequested && voteGranted ){
                    votes.add(msgResp.src)
                    System.err.println("Have votes :${votes}")
                }


            }
        }

    }

    fun sendSyncMsg(msg:String):EchoMsg{
        lock.tryLock(5,TimeUnit.SECONDS)
        System.out.println( msg)
        System.out.flush()
        doesReadValueRec = false
        while(!doesReadValueRec){
            condition.await()
        }
        lock.unlock()
        return voteResp

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
        Thread.sleep((0..100).random().toLong())
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                if(electionDeadline < System.currentTimeMillis())
                if(candidateState != "leader" ){
                    becomeCandidate()
                } else {
                    resetElectionDeadline()
                }

            }
        }, 0, 100)

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
            votedFor = null
        }

    }
    
}