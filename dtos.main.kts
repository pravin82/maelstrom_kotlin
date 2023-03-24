#!/usr/bin/env kotlin

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")


import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

data class EchoMsg(
    val id:Int,
    val dest:String,
    val body:EchoBody,
    val src:String

)

data class OpResult(
    val type:String,
    val value:Int? = null,
    val code:Int? = null,
    val msg:String? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class EchoBody(
    val type:String,
    @JsonProperty("node_id") val nodeId:String? = null,
    @JsonProperty("node_ids") val nodeIds:List<String>? = null,
    @JsonProperty("msg_id")   val msgId:Int? = null,
    @JsonProperty("in_reply_to")   val inReplyTo :Int? = null,
    val key:String? = null,
    val from: Int? = null,
    val to: Int? = null,
    val value:Int? = null,
    val code:Int? = null,
    val term:Int? = null,
    @JsonProperty("vote_granted") val voteGranted:Boolean? = null,
    @JsonProperty("candidate_id") val candidateId:String? = null,
    @JsonProperty("last_log_index") val lastLogIndex:Int? = null,
    @JsonProperty("last_log_term") val lastLogTerm:Int? = null
)

data class LogEntry(
    val term:Int,
    val body:EchoBody?=null
)
data class VoteReqMsg(
    val type:String,
    val term:Int,
    @JsonProperty("candidate_id") val candidateId:String,
    @JsonProperty("last_log_index") val lastLogIndex:Int,
    @JsonProperty("last_log_term") val lastLogTerm:Int

)



data class ReplyBody(
    val type:String,
    @JsonProperty("msg_id")val msgId:Int,
    @JsonProperty("in_reply_to")val inReplyTo:Int
)