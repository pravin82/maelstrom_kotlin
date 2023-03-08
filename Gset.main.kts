#!/usr/bin/env kotlin

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

class Gset(
     val elements: MutableSet<Int?>
){
   private val mapper = jacksonObjectMapper()

    fun addElement(element:Int){
        elements.add(element)
    }
    fun getGsetFrromJson(setStr:String):Gset{
        val elements = mapper.readValue(setStr,  object : TypeReference<Set<Int>>() {})
        return Gset(elements.toMutableSet())
    }
    fun toJson():String{
      return  mapper.writeValueAsString(elements)
    }

    fun merge(gSet:Gset):Gset{
        val mergedElements = gSet.elements union (elements)
        val mergedGset = Gset(mergedElements.toMutableSet())
        return mergedGset
    }

}
