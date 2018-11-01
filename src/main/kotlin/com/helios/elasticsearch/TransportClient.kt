package com.helios.elasticsearch

import com.helios.elasticsearch.documents.StudentDocument
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.transport.client.PreBuiltTransportClient
import java.net.InetSocketAddress

class MyTransportClient {

    var client: TransportClient

    init {
        val settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build()
        client = PreBuiltTransportClient(settings)
        client.addTransportAddress(TransportAddress(InetSocketAddress("localhost", 9300)))
    }


    //索引文档
    fun index(doc: String, indexName: String, indexType: String, id: String? = null) {
        val builder = client.prepareIndex(indexName, indexType)
        id?.let { builder.setId(it) }
        builder.setSource(doc, XContentType.JSON)
                .execute()
                .actionGet()
    }

    //创建索引
    fun createIndex(indexName: String) {
        val existResp = client.admin().indices().prepareExists(indexName).execute().actionGet()
        if (existResp.isExists) {
            client.admin().indices().prepareDelete(indexName)
        }
        client.admin().indices().prepareCreate(indexName).execute().actionGet()
    }

    //判断索引是否存在
    fun existIndex(indexName: String): Boolean {
        return client.admin().indices().prepareExists(indexName).execute().actionGet().isExists
    }

    //获取某个索引文档
    fun getIndex(index: String, type: String, id: String): String {
        val resp = client.prepareGet(index, type, id).execute().actionGet()
        val source: String = resp.sourceAsString
        return source
    }

    //删除某个索引文档
    fun deleteIndex(index: String, type: String, id: String): Boolean {
        val resp = client.prepareDelete(index, type, id).execute().actionGet()
        return resp.result == DocWriteResponse.Result.DELETED
    }

    //更新索引文档
    fun updateIndex(index:String,type: String,id:String,doc: String) {
        client.prepareUpdate(index,type,id)
                .setDoc(doc,XContentType.JSON)
                .execute().actionGet()
    }

    //搜索
    fun search(index: String,type: String,query:QueryBuilder,filter:QueryBuilder,from:Int,size:Int):SearchResponse {
        val resp = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .setPostFilter(filter)
                .setFrom(from)
                .setSize(size)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setExplain(true)
                .execute()
                .actionGet()
        return resp
    }
}


fun main(vararg args: String) {
    val client = MyTransportClient()
    val stu = StudentDocument().apply {
        this.name = "helios"
        this.age = 11
        this.sex = "man"
        this.tag = listOf("hello", "1")
    }
    val stuDoc = stu.toJson()
//    client.index(stuDoc,"student","default","1")
//    client.updateIndex("student","default","1",stuDoc)
    val query = QueryBuilders.matchQuery("name","helios")
    val filter = QueryBuilders.rangeQuery("age").lte(20).gt(10)
    val resp = client.search("student","default",query,filter,0,10)
    val source = resp.hits.hits.firstOrNull()?.sourceAsString
    println()
}