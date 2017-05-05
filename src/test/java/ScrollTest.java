/**
 * ScrollTest.java
 * This file provides a test for reading all records using scrolling
 *
 * @version 0.0.0.1
 * @author  Edgardo Barsallo
 * @modified  maverick-zhn(Servio Palacios)
 * @updated 2017.03.17
 *
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * Do NOT forget to reference the ORIGINAL author of the code.
 */

import org.elasticsearch.index.query.QueryBuilder;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;

import scala.collection.mutable.Map;
import scala.collection.mutable.HashMap;

/* spark */
//import org.apache.spark.rdd.RDD;
import org.trueno.elasticsearch.spark.connector.ElasticClient;
import org.trueno.elasticsearch.spark.connector.SearchObject;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import org.apache.spark.api.java.JavaRDD;
import org.trueno.elasticsearch.spark.connector.Vertex;

public class ScrollTest {

    private static String hostname = "localhost";
    private static String index = "biogrid";
    private static String indexType = "e";
    private static Integer indexSize = 6000;

    /* doTest */
    private static double doTest(ElasticClient client) {
        QueryBuilder qbMatchAll = matchAllQuery();

        /* start benchmark */
        long startTime = System.currentTimeMillis();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
                    objSearch.setIndex(index);
                    objSearch.setType(indexType);
                    objSearch.setSize(indexSize);
                    objSearch.setQuery(qbMatchAll.toString());

        ArrayList<Map<String,Long>> results = client.scroll(objSearch,0,2);

        HashMap<String, Object> r1;

        //JavaRDD<Object>[] results = client.scroll(objSearch);

        /* end benchmark */
        long endTime = System.currentTimeMillis();

        return  (results.size()*1.0)/(endTime - startTime)*1000;
    }//doTest

    /* main */
    public static void main(String[] args) {

        ElasticClient client;

        /* Instantiate the ElasticSearch client and connect to Server */
        client = new ElasticClient("trueno", hostname, 9300);
        client.connect();

        double avg=0.0;

        avg = doTest(client);

        System.out.println("avg: " + avg);

    }//main

}//ScrollTest


