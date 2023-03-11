#!/usr/bin/env kotlin

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")


import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread

data class EchoMsg(
    val id:Int,
    val dest:String,
    val body:EchoBody,
    val src:String

)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class EchoBody(
    val type:String,
    @JsonProperty("node_id") val nodeId:String? = null,
    @JsonProperty("node_ids")  val nodeIds:List<String>? = null,
    @JsonProperty("msg_id")   val msgId:Int? = null,
    @JsonProperty("in_reply_to")   val inReplyTo :Int? = null,
    val echo:String? = null,
    val topology:Map<String,List<String>>? = null,
    val counterMap:MutableMap<String,Int>? = null,
    val delta:Int? = null,
    val txn:List<List<Any?>>? = null,
    val key:String? = null,
    val from: Map<Int, List<Int>>? = null,
    val to: Map<Int, List<Int>>? = null,
    val value:Map<Int,List<Int>>? = null,
    val code:Int? = null,
    val text:String? = null,
      @JsonProperty("create_if_not_exists")   val createIfNotExists :Boolean? = null,
)



data class ReplyBody(
    val type:String,
    @JsonProperty("msg_id")val msgId:Int,
    @JsonProperty("in_reply_to")val inReplyTo:Int
)