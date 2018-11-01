package com.helios.elasticsearch.documents

import com.google.gson.Gson

class StudentDocument {
    var name: String? = null
    var sex: String? = null
    var age: Int? = null
    var tag: List<String>? = null

    fun toJson(): String {
        return Gson().toJson(this)
    }
}