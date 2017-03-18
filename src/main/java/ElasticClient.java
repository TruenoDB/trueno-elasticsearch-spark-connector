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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;

public class ElasticClient {

    /* Private properties */
    private TransportClient client;
    private String clusterName;
    private String[] addresses;
    private final int scrollTimeOut = 60000;
    private final int hitsPerShard = 100;
    private final String strSource = "_source";

    /**
     * Constructor
     * @param clusterName -> String
     * @param addresses -> String
     */
    public ElasticClient(String clusterName, String addresses) {
        /* set cluster name and addresses */
        this.clusterName = clusterName;
        this.addresses = addresses.split(",");
    }

    /**
     * connect to elasticsearch using transport client
     */
    public void connect() {

        try{
            /* prepare cluster settings */
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", this.clusterName)
                    .build();

            /* instantiate transport build */
            this.client = TransportClient.builder().settings(settings).build();

            /* set addresses */
            for(String addr: this.addresses){
                this.client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress( InetAddress.getByName(addr), 9300)));
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
    public Map<String,Object>[] search(SearchObject data) {

        /* collecting results */
        ArrayList<Map<String,Object>> sources = new ArrayList<>();

        try{

            SearchRequestBuilder srBuilder = this.client.prepareSearch(data.getIndex())
                    .setTypes(data.getType())
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setSize(data.getSize())
                    .setQuery(QueryBuilders.wrapperQuery(data.getQuery()));

            SearchResponse resp = srBuilder.get();

            SearchHit[] results = resp.getHits().getHits();

            //System.out.println("Hits are " + results.length);

            /* for each hit result */
            for(SearchHit hit: results){

                /* add map to array, note: a map is the equivalent of a JSON object */
                sources.add(ImmutableMap.of(this.strSource, hit.getSource()));
            }

            return sources.toArray(new Map[sources.size()]);

        }catch (Exception e){
            System.out.println(e);
        }
        return new Map[0];
    }

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

    /**
     * The Scroll API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * @param data -> SearchObject
     * @return results -> ArrayList
     */
    public Map<String,Object>[] scroll(SearchObject data) {

        TimeValue tvScrollTime  = new TimeValue(this.scrollTimeOut);

        /* collect results in array */
        ArrayList<Map<String,Object>> alSources = new ArrayList<>();

        SearchResponse scrollResp = this.client.prepareSearch(data.getIndex())
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(tvScrollTime)
                .setQuery(data.getQuery())
                .setSize(this.hitsPerShard).execute().actionGet(); //100 hits per shard will be returned for each scroll

        //Scroll until no hits are returned
        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //hit returned
                //System.out.println(hit.getSource());
                alSources.add(ImmutableMap.of(this.strSource, hit.getSource()));
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tvScrollTime).execute().actionGet();

            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                return alSources.toArray(new Map[alSources.size()]);
            }
        }//while

    }//scroll

}//class


