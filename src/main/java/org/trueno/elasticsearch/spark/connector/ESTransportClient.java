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

/* ElasticSearch dependencies */
import org.elasticsearch.index.query.QueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.SearchHit;

/* Spark - Apache */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.commons.lang.StringUtils;

/* Utils */
import scala.collection.mutable.Map;
import scala.collection.mutable.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import scala.collection.JavaConverters.*;

/* Inmutable Map and Lists */
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

public class ESTransportClient{

    private static String hostname = "localhost";
    private static String index = "biogrid";
    private static String indexTypeVertex = "v";
    private static String indexTypeEdge = "e";
    private static Integer indexSize = 100;
    private final String strSource = "_source";
    private final int scrollTimeOut = 60000;
    private final String strClusterName = "trueno";
    private ElasticClient client;
    private JavaSparkContext jsc;

    /* Elastic Search Transport Client Class Constructor */
    public ESTransportClient(String strIndex, JavaSparkContext psc) {

        index = strIndex;

        /* Instantiate the ElasticSearch client and connect to Server */
        client = new ElasticClient(strClusterName, hostname);
        client.connect();

        jsc = psc;

        System.out.println("Connected to the Elastic Search Client ... ");

    }//Constructor

    /* getVertexRDD JavaRDD */
    public JavaRDD<Map<String,Long>> getVertexRDD() {

        System.out.println("Retrieving vertices JavaRDD[Map<String,Long>]... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeVertex);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Map<String,Long>> results = client.scroll(objSearch);

        System.out.println("Retrieved Vertices: [" + results.size() + "]");

        JavaRDD<Map<String,Long>> rddResults = jsc.parallelize(results);

        return rddResults;

    }//getVertexRDD

    /* getVertexRDD JavaRDD[Long] */
    public JavaRDD<Long> getLongVertexRDD() {

        //System.out.println("Retrieving vertices JavaRDD[Long] ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeVertex);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Long> results = client.scrollVertex(objSearch);

        //System.out.println("Retrieved Vertices [" + results.size() + "]");

        JavaRDD<Long> rddResults = jsc.parallelize(results);

        return rddResults;

    }//getVertexRDD JavaRDD[Long]

    /* getVertexRDD ArrayList*/
    public ArrayList<Map<String,Long>> getVertexArrayList() {

        System.out.println("Retrieving vertices Array List ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeVertex);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

         /* get results */
        ArrayList<Map<String,Long>> results = client.scroll(objSearch);

        System.out.println("Retrieved Vertices: [" + results.size() + "]");

        return results;

    }//getVertexRDD ArrayList

    /* getEdgeRDD JavaRDD */
    public JavaRDD<Map<Long,Long>> getEdgeRDD() {

        //System.out.println("Retrieving edges JavaRDD[Map<Long,Long>] ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeEdge);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Map<Long,Long>> results = client.scrollEdge(objSearch);

        //System.out.println("Retrieved Edges: [" + results.size() + "]");

        JavaRDD<Map<Long,Long>> rddResults = jsc.parallelize(results);

        return rddResults;

    }//getEdgeRDD

    /* getEdgeRDD JavaRDD[HashMap]*/
    public JavaRDD<Map<Long,Long>> getEdgeRDDHashMap() {

        System.out.println("Retrieving edges JavaRDD[Map<Long,Long>] ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeEdge);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        Map<Long,Long> results = client.scrollEdgeHashMap(objSearch);

        System.out.println("Retrieved Edges: [" + results.size() + "]");

        JavaRDD<Map<Long,Long>> rddResults = jsc.parallelize(ImmutableList.of(results));

        return rddResults;

    }//getEdgeRDDHashMap

    /* getEdgeRDD ArrayList*/
    public ArrayList<Map<Long,Long>> alGetEdgeRDD() {

        System.out.println("Retrieving edges ArrayList ... ");

        QueryBuilder qbMatchAll = matchAllQuery();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                                    objSearch.setIndex(index);
                                    objSearch.setType(indexTypeEdge);
                                    objSearch.setSize(indexSize);
                                    objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        ArrayList<Map<Long,Long>> results = client.scrollEdge(objSearch);

        System.out.println("Retrieved Edges: [" + results.size() + "]");

        return results;

    }//getEdgeRDD ArrayList

    /**
     * TEST DATA ---------------------------------------------------------------------->
     * @return
     */
    public JavaRDD<Map<Long,Long>> scalaMapLLEdgeFakeData() {

        System.out.println("Generating fake data ... ");

        Map<Long, Long> numbers1 = new HashMap<Long, Long>();

        numbers1.put(new Long(1),new Long(2));
        numbers1.put(new Long(3),new Long(4));
        numbers1.put(new Long(5),new Long(6));
        numbers1.put(new Long(7),new Long(8));
        numbers1.put(new Long(9),new Long(10));

        JavaRDD<Map<Long, Long>> javaRDD = jsc.parallelize(ImmutableList.of(numbers1));

        return javaRDD;

    }//scalaMapLLEdgeFakeData

    public JavaRDD<Long> longMapVertexFakeData() {

        System.out.println("Generating fake data ... ");

        Long numbers1 = new Long(1);
        Long numbers2 = new Long(2);
        Long numbers3 = new Long(3);

        JavaRDD<Long> javaRDD = jsc.parallelize(ImmutableList.of(numbers1, numbers2, numbers3));

        return javaRDD;

    }//longMapVertexFakeData

}//class