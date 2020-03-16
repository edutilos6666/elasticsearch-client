package org.edutilos.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * Created by Nijat Aghayev on 16.03.20.
 */
public class SearchScrollAPIExample {
    private static RestHighLevelClient client;
    public static void main(String[] args) {
        // connect
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        try {
            example1();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void example1() throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest request = new SearchRequest("bank");
        request.scroll(scroll);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(5); // default 10
        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();
        SearchHit[] searchHits = response.getHits().getHits();
        processSearchHits(searchHits);
        int counter = 1;
        while(searchHits != null && searchHits.length > 0){
            System.out.printf("Scroll step = %d\n", counter++);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            searchHits = response.getHits().getHits();
            processSearchHits(searchHits);
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        System.out.printf("clearScrollResponse.isSucceeded() = %s\n", clearScrollResponse.isSucceeded());
        System.out.printf("clearScrollResponse.isFragment() = %s\n", clearScrollResponse.isFragment());
    }

    private static void processSearchHits(SearchHit[] searchHits) {
        for(SearchHit searchHit: searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
        System.out.println();
    }
}

















