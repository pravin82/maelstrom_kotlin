#!/usr/bin/env kotlin

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

class GCounter(
     val counterMap: MutableMap<String, Int>,
     val nodeId:String
){
   private val mapper = jacksonObjectMapper()

    fun addElement(increment:Int){
        val counter = counterMap.get(nodeId)
        counterMap.put(nodeId,(counter?:0).plus(increment))
    }


    fun merge(gCounter:GCounter){
        val otherCounterMap = gCounter.counterMap
        otherCounterMap.map{
            val otherVal = otherCounterMap.get(it.key)?:0
            val value = maxOf(otherVal, counterMap.get(it.key)?:0)
            counterMap.put(it.key ,value)
        }
    }

    fun read():Int{
      return  counterMap.values.sum()
    }

}
