package org.edutilos.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Created by Nijat Aghayev on 16.03.20.
 */

// executes multiple search requests parallel
public class MultiSearchAPIExample {
    private static RestHighLevelClient client;
    public static void main(String[] args) {
        //connect
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        // disconnect
        try {
            example1();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void example1() throws IOException {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.add(constructSearchRequestForSort());
        multiSearchRequest.add(constructSearchRequestForIndexPosts());
        multiSearchRequest.add(constructSearchRequestForAggregation());

        // execute and get MultiSearchResponse
        MultiSearchResponse responses = client.msearch(multiSearchRequest, RequestOptions.DEFAULT);
        MultiSearchResponse.Item[] responseItems = responses.getResponses();
        for(int i=0; i< responseItems.length; ++i) {
            processSearchResponse(i, responseItems[i]);
        }
    }

    private static void processSearchResponse(int index, MultiSearchResponse.Item responseItem) {
        if(index == 0) {
            processSearchResponseForSort(responseItem);
        } else if(index == 1) {
            processSearchRequestForIndexPosts(responseItem);
        } else if(index == 2) {
            processSearchRequestForAggregation(responseItem);
        }
    }

    private static void processSearchResponseForSort(MultiSearchResponse.Item responseItem) {
        if(responseItem.isFailure()) {
            responseItem.getFailure().printStackTrace();
        } else {
            SearchResponse response = responseItem.getResponse();
            SearchHits searchHits = response.getHits();
            System.out.println("<<searchAPI with score sort and field sort (balance, desc)>>");
            searchHits.forEach(one-> {
                System.out.println(one.getSourceAsString());
            });
            System.out.println();
        }
    }

    private static void processSearchRequestForIndexPosts(MultiSearchResponse.Item responseItem) {
        if(responseItem.isFailure()) {
            System.out.println(responseItem.getFailureMessage());
        } else {
            SearchResponse response = responseItem.getResponse();
            System.out.println("<<searcAPI [document = posts, from = 0, size = 100, termQuery(user = kimchy)]>>");
            response.getHits().forEach(one-> {
                System.out.println(one.getSourceAsString());
            });
            System.out.println();
        }
    }

    private static void processSearchRequestForAggregation(MultiSearchResponse.Item responseItem) {
        if(responseItem.isFailure()) {
            responseItem.getFailure().printStackTrace();
        } else {
            SearchResponse response = responseItem.getResponse();
            System.out.println("<<searchAPI [with aggregation]>>");
            response.getHits().forEach(one-> {
                System.out.println(one.toString());
            });
            Aggregations aggregations = response.getAggregations();
            Terms terms = aggregations.get("group_by_state");
            // following will throw exception , because we have requested TermsAggregation
//        Range range = aggregations.get("group_by_state");
            System.out.println("<<All Buckets>>");
            terms.getBuckets().forEach(one-> {
                String bucketKey = one.getKey().toString();
                long docCount = one.getDocCount();
                Avg averageBalance = one.getAggregations().get("average_balance");
                double averageBalanceValue = averageBalance.getValue();
                System.out.printf("key = %s, docCount = %s, averageBalance = %s\n", bucketKey, docCount,
                        averageBalanceValue);
            });
            System.out.println();
        }
    }

    private static SearchRequest constructSearchRequestForSort() {
        SearchRequest request = new SearchRequest("bank");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // Field-, Score-, GeoDistance- and ScriptSortBuilder
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("balance").order(SortOrder.ASC));
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);
        return request;
    }

    private static SearchRequest constructSearchRequestForIndexPosts() {
        SearchRequest request = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(100);
        searchSourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        request.source(searchSourceBuilder);
        return request;
    }

    private static SearchRequest constructSearchRequestForAggregation() {
        // aggregation
        SearchRequest request = new SearchRequest("bank");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("group_by_state")
                .field("state.keyword");
        aggregation.subAggregation(AggregationBuilders.avg("average_balance")
                .field("balance"));
        searchSourceBuilder.aggregation(aggregation);
        request.source(searchSourceBuilder);
        return request;
    }
}
