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
@JsonInclude(JsonInclude.Include.NON_NULL)
data class EchoBody(
    val type:String,
    @JsonProperty("node_id") val nodeId:String? = null,
    @JsonProperty("node_ids")  val nodeIds:List<String>? = null,
    @JsonProperty("msg_id")   val msgId:Int,
    @JsonProperty("in_reply_to")   val inReplyTo :Int? = null,
    val echo:String? = null,
    val topology:Map<String,List<String>>? = null,
    val element:Int? = null,
    val value:Set<Int?>? = null
)

data class ReplyBody(
    val type:String,
    @JsonProperty("msg_id")val msgId:Int,
    @JsonProperty("in_reply_to")val inReplyTo:Int
)