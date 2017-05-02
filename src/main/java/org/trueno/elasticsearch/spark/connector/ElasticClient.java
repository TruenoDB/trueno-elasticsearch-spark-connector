/**
 * ElasticClient.java
 * This file connects to an elasticSearch Cluster
 * includes connect(), search(), bulk(), and scroll() operations
 *
 * @version 0.0.0.1
 * @author  Victor, maverick-zhn(Servio Palacios)
 * @updated 2017.03.17
 *
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * Do NOT forget to reference the ORIGINAL author of the code.
 */
package org.trueno.elasticsearch.spark.connector;

/* Immutable Map and Lists */
import com.google.common.collect.ImmutableMap;

/* ElasticSearch dependencies */
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortOrder;

/* spark dependencies */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.io.PrintStream;
import java.util.ArrayList;

import org.elasticsearch.transport.client.PreBuiltTransportClient;
import scala.collection.mutable.Map;
import scala.collection.mutable.HashMap;

import scala.collection.JavaConverters.*;
import scala.collection.JavaConversions.*;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.netlib.lapack.Slacon.i;

public class ElasticClient {

    /* Private properties */
    private TransportClient client;
    private String clusterName;
    private String[] addresses;
    private Integer intTransportPort = 9300;
    private final int scrollTimeOut = 60000;
    private final int hitsPerShard = 1000;
    private final String strSource = "_source";
    private final String strId = "id";
    private final String strEdgeSource = "source";
    private final String strEdgeTarget = "target";

    /**
     * Constructor
     * @param clusterName -> String
     * @param addresses -> String
     */
    public ElasticClient(String clusterName, String addresses, Integer pintTransportPort) {
        /* set cluster name and addresses */
        this.clusterName = clusterName;
        this.addresses = addresses.split(",");
        intTransportPort = pintTransportPort;
    }

    /**
     * connect to elasticsearch using transport client
     */
    public void connect() {

        try{
            /* prepare cluster settings */
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName)
                    .build();

            /* instantiate transport build */
            //this.client = TransportClient.builder().settings(settings).build();

            this.client = (new PreBuiltTransportClient(settings));

            /* set addresses */
            for(String addr: addresses){
                this.client.addTransportAddress(new InetSocketTransportAddress(
                            new InetSocketAddress( InetAddress.getByName(addr), intTransportPort )));
            }

        }catch (Exception e){
            System.out.println(e);
        }
    }

    /**
     * The search API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * @param data -> SearchObject
     * @return results -> ArrayList
     */
//    public Map<String,Object>[] search(SearchObject data) {
//
//        /* collecting results */
//        ArrayList<Map<String,Object>> sources = new ArrayList<>();
//
//        try{
//
//            SearchRequestBuilder srBuilder = this.client.prepareSearch(data.getIndex())
//                    .setTypes(data.getType())
//                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                    .setSize(data.getSize())
//                    .setQuery(QueryBuilders.wrapperQuery(data.getQuery()));
//
//            SearchResponse resp = srBuilder.get();
//
//            SearchHit[] results = resp.getHits().getHits();
//
//            //System.out.println("Hits are " + results.length);
//
//            /* for each hit result */
//            for(SearchHit hit: results){
//
//                /* add map to array, note: a map is the equivalent of a JSON object */
//                sources.add(ImmutableMap.of(this.strSource, hit.getSource()));
//            }
//
//            return sources.toArray(new Map[sources.size()]);
//
//        }catch (Exception e){
//            System.out.println(e);
//        }
//        return new Map[0];
//    }

    /**
     * The bulk API allows one to index and delete several documents in a single request.
     * @param bulkData -> BulkObject [Index, Operations[][]]
     * @return [batch finished] -> String
     */
    public String bulk(BulkObject bulkData) {
        try {
            /* we will use this index instance on ES */
            String index = bulkData.getIndex();

            /* requested batch operations from client */
            String[][] operations = bulkData.getOperations();

            long totalStartTime = System.currentTimeMillis();

            BulkRequestBuilder bulkRequest = this.client.prepareBulk();

            for (String[] info : operations) {

                if (info[0].equals("index")) {
                    /*
                    info[0] = index or delete
                    info[1] = type {v, e}
                    info[2] = id
                    info[3] = '{name:pedro,age:15}'
                     */
                    /* adding document to the batch */
                    bulkRequest.add(this.client.prepareIndex(index, info[1], info[2]).setSource(info[3]));

                    continue;
                }//if

                if (!info[0].equals("delete")) continue;

                /* adding document to the batch */
                bulkRequest.add(this.client.prepareDelete(index, info[1], info[2]));

            }//for

            BulkResponse bulkResponse = bulkRequest.get();

            long totalEstimatedTime = System.currentTimeMillis() - totalStartTime;

            System.out.println("batch time ms: " + totalEstimatedTime);

            if (bulkResponse.hasFailures()) {
                return bulkResponse.buildFailureMessage();
            }

            return "[]";
        }
        catch (Exception e) {
            e.printStackTrace(new PrintStream(System.out));
            return null;
        }
    }//bulk

    /* Get an specific value from object */
    private Long getFromSource(Object objSource, String strField){

        java.util.HashMap hashMap = (java.util.HashMap) objSource;
        Object objID = hashMap.get(strField);
        Long lngResult = null;

        if(objID != null) {

            lngResult = Long.parseLong(objID.toString());

            if (lngResult == null) {
                System.out.println("Error converting Long");
                lngResult = new Long(0);
            }

            //System.out.println(lngResult.toString());
        }

        return lngResult;

    }//getIdFromSource

    /**
     * The Scroll API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * @param data -> SearchObject
     * @return results -> ArrayList
     */
    public ArrayList<Map<String,Long>> scroll(SearchObject data, Integer id, Integer max) {

        TimeValue tvScrollTime  = new TimeValue(this.scrollTimeOut);

        boolean boolJustOnce = true;
        Integer intCount = 0;

        /* collect results in array */
        ArrayList<Map<String,Long>> sparkSources = new ArrayList<>();

        ArrayList<Map<String,Object>> sources = new ArrayList<>();

        // .setFetchSource(new String[]{strFields}, null)
        System.out.println("Index " + data.getIndex());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(Integer.MAX_VALUE);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.slice(new SliceBuilder(id, max));

        //.setFetchSource(new String[]{strId}, null)

        SearchResponse scrollResp = client.prepareSearch(data.getIndex())
                                    .setSource(searchSourceBuilder)
                                    .setTypes(data.getType())
                                    .setScroll(tvScrollTime)
                                    .setSize(hitsPerShard).execute().actionGet(); //n hits per shard will be returned for each scroll

        // Scroll until no hits are returned
        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {

                intCount++;
                //hit returned
                if (boolJustOnce) {
                    boolJustOnce = false;
                    System.out.println(hit.getSource());
                }

                //sources.add(ImmutableMap.of(strSource,hit.getSource()));

                Long lngId = getFromSource(hit.getSource(), strId);
                if (lngId != null) {
                    Map<String, Long> hmVertex = new HashMap<String, Long>();
                    hmVertex.put(strId,lngId);
                    sparkSources.add(hmVertex);
                }

            }//for

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tvScrollTime).execute().actionGet();

            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                System.out.println("Results count " + intCount);
                return sparkSources;//.toArray(new Map[sparkSources.size()]);
            }

        }//while

    }//scroll

    /**
     * The Scroll API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * @param data -> SearchObject
     * @return results -> ArrayList
     */
//    public ArrayList<Long> scrollVertex(SearchObject data) {
//
//        TimeValue tvScrollTime  = new TimeValue(this.scrollTimeOut);
//
//        boolean boolJustOnce = true;
//
//        /* collect results in array */
//        ArrayList<Long> sparkSources = new ArrayList<>();
//
//        // .setFetchSource(new String[]{strFields}, null)
//        //System.out.println("Index " + data.getIndex() + " type " + data.getType());
//
//        //.setFetchSource(new String[]{strId}, null)
//        //.setQuery(data.getQuery())
//        //after scroll
//
//        SearchResponse scrollResp = this.client.prepareSearch(data.getIndex())
//                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
//                .setTypes(data.getType())
//                .setScroll(tvScrollTime)
//                .setQuery(data.getQuery())
//                .setSize(this.hitsPerShard).execute().actionGet(); //n hits per shard will be returned for each scroll
//
//        //Scroll until no hits are returned
//        while (true) {
//
//            for (SearchHit hit : scrollResp.getHits().getHits()) {
//                //hit returned
//                if(boolJustOnce) {
//                    boolJustOnce = false;
//                    //System.out.println(hit.getSource());
//                }
//
//                Long lngId = getFromSource(hit.getSource(), strId);
//                if (lngId != null) {
//                    sparkSources.add(lngId);
//                }
//
//            }//for
//
//            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tvScrollTime).execute().actionGet();
//
//            //Break condition: No hits are returned
//            if (scrollResp.getHits().getHits().length == 0) {
//                return sparkSources;
//            }
//        }//while
//
//    }//scroll
//
//    /**
//     * The Scroll API allows you to execute a search query and get back search hits that match the query.
//     * The query can either be provided using a simple query string as a parameter, or using a request body
//     * @param data -> SearchObject
//     * @return results -> ArrayList
//     */
//    public ArrayList<Map<Long,Long>> scrollEdge(SearchObject data) {
//
//        TimeValue tvScrollTime  = new TimeValue(scrollTimeOut);
//
//        boolean boolJustOnce = true;
//
//        /* collect results in array */
//        ArrayList<Map<Long,Long>> sparkSources = new ArrayList<>();
//
//        //  .setFetchSource(new String[]{strEdgeSource,strEdgeTarget}, null)
//        SearchResponse scrollResp = this.client.prepareSearch(data.getIndex())
//                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
//                .setTypes(data.getType())
//                .setScroll(tvScrollTime)
//                .setQuery(QueryBuilders.wrapperQuery(data.getQuery()))
//                .setSize(hitsPerShard).execute().actionGet(); //n hits per shard will be returned for each scroll
//
//        //Scroll until no hits are returned
//        while (true) {
//
//            for (SearchHit hit : scrollResp.getHits().getHits()) {
//                //hit returned
//                if(boolJustOnce) {
//                    boolJustOnce = false;
//                    //System.out.println(hit.getSource());
//                }
//
//                Long lngSource = getFromSource(hit.getSource(), strEdgeSource);
//                Long lngTarget = getFromSource(hit.getSource(), strEdgeTarget);
//
//                Map<Long, Long> hmEdge = new HashMap<Long, Long>();
//
//                if (lngSource != null && lngTarget != null) {
//                    hmEdge.put(lngSource,lngTarget);
//                    sparkSources.add(hmEdge);
//                }
//
//            }//for
//
//            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tvScrollTime).execute().actionGet();
//
//            //Break condition: No hits are returned
//            if (scrollResp.getHits().getHits().length == 0) {
//                return sparkSources;
//            }
//        }//while
//
//    }//scroll

    /**
     * The Scroll API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * @param data -> SearchObject
     * @return results -> ArrayList
     */
//    public Map<Long,Long> scrollEdgeHashMap(SearchObject data) {
//
//        TimeValue tvScrollTime  = new TimeValue(scrollTimeOut);
//
//        boolean boolJustOnce = true;
//
//        /* collect results in array */
//        ArrayList<Map<Long,Long>> sparkSources = new ArrayList<>();
//
//        Map<Long, Long> hmEdge = new HashMap<Long, Long>();
//
//        // .setFetchSource(new String[]{strFields}, null)
//        //  .setFetchSource(new String[]{strEdgeSource,strEdgeTarget}, null)
//        SearchResponse scrollResp = this.client.prepareSearch(data.getIndex())
//                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
//                .setTypes(data.getType())
//                .setScroll(tvScrollTime)
//                .setQuery(data.getQuery())
//                .setSize(hitsPerShard).execute().actionGet(); //n hits per shard will be returned for each scroll
//
//        //Scroll until no hits are returned
//        while (true) {
//
//            for (SearchHit hit : scrollResp.getHits().getHits()) {
//                //hit returned
//                if(boolJustOnce) {
//                    boolJustOnce = false;
//                    System.out.println(hit);
//                }
//
//                Long lngSource = getFromSource(hit.getSource(), strEdgeSource);
//                Long lngTarget = getFromSource(hit.getSource(), strEdgeTarget);
//
//                if (lngSource != null && lngTarget != null) {
//                    hmEdge.put(lngSource,lngTarget);
//                    sparkSources.add(hmEdge);
//                }
//
//            }//for
//
//            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tvScrollTime).execute().actionGet();
//
//            //Break condition: No hits are returned
//            if (scrollResp.getHits().getHits().length == 0) {
//                return hmEdge;
//            }
//        }//while
//
//    }//scroll

    /**
     * The search API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * addListener() Add an action listener to be invoked when a response has received.
     * @param data -> SearchObject
     * @return results -> promise
     */
//    public ListenableActionFuture<SearchResponse> lafScroll(SearchObject data) {
//
//        /* collecting results */
//        TimeValue tvScrollTime  = new TimeValue(scrollTimeOut);
//
//        //.addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
//        ListenableActionFuture<SearchResponse> scrollResponse = this.client.prepareSearch(data.getIndex())
//                .setTypes(data.getType())
//                .setScroll(tvScrollTime)
//                .setQuery(data.getQuery())
//                .setSize(this.hitsPerShard).execute(); //100 hits per shard will be returned for each scroll
//
//
//        return scrollResponse;
//
//    }//lafScroll

}//class


