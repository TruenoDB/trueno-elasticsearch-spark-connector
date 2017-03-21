/**
 * TransportClient.java
 * This file connects to an elasticSearch Cluster
 * includes connect(), search(), bulk(), and scroll() operations
 *
 * @version 0.0.0.1
 * @author  maverick-zhn(Servio Palacios)
 * @updated 2017.03.17
 *
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * Do NOT forget to reference the ORIGINAL author of the code.
 */

package org.trueno.elasticsearch.spark.connector;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Map;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.SearchHit;

/* Spark */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;


import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import scala.collection.JavaConverters.*;

import com.google.common.collect.ImmutableMap;

public class ESTransportClient{

    private static String hostname = "localhost";
    private static String index = "biogrid";
    private static String indexTypeVertex = "v";
    private static String indexTypeEdge = "e";
    private static Integer indexSize = 6000;
    private final String strSource = "_source";
    private final int scrollTimeOut = 60000;
    private final String strClusterName = "trueno";
    private ElasticClient client;

    public ESTransportClient(String strIndex) {

        index = strIndex;

        /* Instantiate the ElasticSearch client and connect to Server */
        client = new ElasticClient(strClusterName, hostname);
        client.connect();

        System.out.println("Connected to the Elastic Search Client ... ");

    }//Constructor

    /* indexCall */
    private static ArrayList<Map<String,Long>> indexCall(ElasticClient client, String strIndexType) {
        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
        objSearch.setIndex(index);
        objSearch.setType(strIndexType);
        objSearch.setSize(indexSize);
        objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Map<String,Long>> results = client.scroll(objSearch);

        return results;

    }//indexCall

    /* doScrollTest */
    private static double doScrollTest(ElasticClient client, String strIndexType) {
        QueryBuilder qbMatchAll = matchAllQuery();

        /* start benchmark */
        long startTime = System.currentTimeMillis();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
        objSearch.setIndex(index);
        objSearch.setType(indexTypeVertex);
        objSearch.setSize(indexSize);
        objSearch.setQuery(qbMatchAll.toString());

        client.lafScroll(objSearch).addListener(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse scrollResp) {

                /* Collecting results */
                ArrayList<Map<String, Object>> alSources = new ArrayList<>();

                SearchHit[] results = scrollResp.getHits().getHits();

                for (SearchHit hit : results) {
                    //hit returned
                    System.out.println(hit.getSource());
                    alSources.add(ImmutableMap.of("_source", hit.getSource()));
                }//for

            }//onResponse

            /* end benchmark */
            long endTime = System.currentTimeMillis();

            @Override
            public void onFailure(Throwable throwable) {

                System.out.println("Failed on scroll.");

            }
        });

        return 0;
    }//indexCall

    /* main */
    public static void main(String[] args) {

        ElasticClient client;

        /* Instantiate the ElasticSearch client and connect to Server */
        client = new ElasticClient("trueno", hostname);
        client.connect();

        ArrayList<Map<String,Long>> vertices =  indexCall(client, indexTypeEdge);

        System.out.println("Vertices: " + vertices.size());

    }//main

    /* getVertexRDD */
    //public ArrayList<Map<String,Long>> getVertexRDD() {
    public JavaRDD<Map<String,Long>> getVertexRDD(JavaSparkContext sc) {

        System.out.println("Retrieving vertices ... ");

        ArrayList<Map<String,Long>> results = indexCall(client, indexTypeVertex);

        System.out.println("Retrieved Vertices: [" + results.size() + "]");

        JavaRDD<Map<String,Long>> rddResults = sc.parallelize(results);

        return rddResults;

    }//getVertexRDD

    /* getEdgeRDD */
    //public ArrayList<Map<String,Long>> getEdgeRDD() {
    public JavaRDD<Map<Long,Long>> getEdgeRDD(JavaSparkContext sc) {

        System.out.println("Retrieving edges ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeEdge);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Map<Long,Long>> results = client.rddEdgeScroll(objSearch);

        //ArrayList<Map<Long,Long>> results = indexCall(client, indexTypeEdge);

        System.out.println("Retrieved Edges: [" + results.size() + "]");

        JavaRDD<Map<Long,Long>> rddResults = sc.parallelize(results);

        return rddResults;

    }//getEdgeRDD

    /* getGraph */
    //public RDD<Integer> getGraph(JavaSparkContext sc) {
    public JavaRDD<Map<String,Long>> getGraph(JavaSparkContext sc) {

        System.out.println("Retrieving graph ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeVertex);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Map<String,Long>> results = client.scroll(objSearch);

        JavaRDD<Map<String,Long>> rddResults = sc.parallelize(results);

        return rddResults;

    }//getGraph

    /* Testing Scroll with ListenableActionFuture */
    public void scrollTest(){

        double avg=0.0;

        avg = doScrollTest(client, indexTypeVertex);

        System.out.println("avg: " + avg);

    }

}//class