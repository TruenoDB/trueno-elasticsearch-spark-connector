/**
 * SearchObject.java
 * This file provides an object for the search operation in ES
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
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class ScrollTest {

    private static String hostname = "localhost";
    private static String index = "movies";
    private static String indexType = "v";
    private static Integer indexSize = 6000;

    /* doTest */
    private static double doTest(ElasticClient client) {

        int count = 0;
        QueryBuilder qbMatchAll = matchAllQuery();

        /* start benchmark */
        long startTime = System.currentTimeMillis();

        /* prepare search object */
        SearchObject objSearch = new SearchObject();
        objSearch.setIndex(index);
        objSearch.setType(indexType);
        objSearch.setSize(indexSize);
        objSearch.setQuery(qbMatchAll.toString());

        /* get results */
        Map<String,Object>[] results = client.scroll(objSearch);

        /* end benchmark */
        long endTime = System.currentTimeMillis();

        return  (results.length*1.0)/(endTime - startTime)*1000;
    }//doTest

    /* main */
    public static void main(String[] args) {

        ElasticClient client;

        /* Instantiate the ElasticSearch client and connect to Server */
        client = new ElasticClient("trueno", hostname);
        client.connect();

        double avg=0.0;
        //for (int i=0; i<10; i++) {
        avg += doTest(client);
        //}
        System.out.println("avg: " + avg);

    }//main

}//ScrollTest


