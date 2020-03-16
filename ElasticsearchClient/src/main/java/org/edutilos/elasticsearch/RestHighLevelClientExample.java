package org.edutilos.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Nijat Aghayev on 15.03.20.
 */
public class RestHighLevelClientExample {
    private static RestHighLevelClient client;
    public static void main(String[] args) {
        // connect
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));


       try {
           indexAPI();
           getAPI();
           existsAPI();
           deleteAPI();
           updateAPI();
           searchAPI();
       } catch(IOException ex) {
           ex.printStackTrace();
       } finally {
           // disconnect
           try {
               client.close();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }


    }


    private static void searchAPI() throws IOException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // find all indices
        // first 10 hits
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // do not fetch document itself
        searchSourceBuilder.fetchSource(false);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        int totalShards = response.getTotalShards();
        int successfulShards = response.getSuccessfulShards();
        int failedShards = response.getFailedShards();
        SearchHits searchHits = response.getHits();
        System.out.println("<<searchAPI>>");
        searchHits.forEach(one-> {
            System.out.println(one.getSourceAsString());
        });


        //specify sorting
        request = new SearchRequest("bank");
        searchSourceBuilder = new SearchSourceBuilder();
        // Field-, Score-, GeoDistance- and ScriptSortBuilder
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("balance").order(SortOrder.ASC));
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        searchHits = response.getHits();
        System.out.println("<<searchAPI with score sort and field sort (balance, desc)>>");
        searchHits.forEach(one-> {
            System.out.println(one.getSourceAsString());
        });


        // from size
        request = new SearchRequest();
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(1000);
        searchSourceBuilder.size(100);
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("<<searcAPI [from = 1000, size = 100]>>");
        response.getHits().forEach(one-> {
            System.out.println( one.getSourceAsString());
        });


        // find index posts
        request.indices("posts");
        searchSourceBuilder = new SearchSourceBuilder();
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("<<searcAPI [document = posts]>>");
        response.getHits().forEach(one-> {
            System.out.println( one.getSourceAsString());
        });

        request = new SearchRequest("posts");
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(100);
        searchSourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("<<searcAPI [document = posts, from = 0, size = 100, termQuery(user = kimchy)]>>");
        response.getHits().forEach(one-> {
            System.out.println(one.getSourceAsString());
        });

        // highlighting
        request = new SearchRequest("bank");
        searchSourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightAccountNumber = new HighlightBuilder.Field("account_number");
        highlightAccountNumber.highlighterType("unified");

        HighlightBuilder.Field highlightBalance = new HighlightBuilder.Field("balance");
        highlightBuilder.field(highlightAccountNumber).field(highlightBalance);
        searchSourceBuilder.highlighter(highlightBuilder);

        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("<<searchAPI [with highlighter]>>");
        response.getHits().forEach(one-> {
            System.out.println(one.getSourceAsString());
            System.out.println("HIGHLIGHTED FIELDS");
            one.getHighlightFields().forEach((key, value)-> {
                System.out.printf("%s = %s , ", key, value.toString());
            });
            System.out.println();
        });


        // aggregation
        request = new SearchRequest("bank");
        searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("group_by_state")
                .field("state.keyword");
        aggregation.subAggregation(AggregationBuilders.avg("average_balance")
                .field("balance"));
        searchSourceBuilder.aggregation(aggregation);
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
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


        // suggestion
        request = new SearchRequest("bank");
        searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("firstname")
                .text("Opal");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_firstname", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        Suggest suggest =  response.getSuggest();
        TermSuggestion suggestFirstname = suggest.getSuggestion("suggest_firstname");
        String suggestName = suggestFirstname.getName();
        suggestFirstname.getEntries().forEach(one-> {
            System.out.printf("%s => %s\n", suggestName, one.getText().toString());
        });

        // profiling
        request = new SearchRequest("bank");
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.profile(true);
        request.source(searchSourceBuilder);
        response = client.search(request, RequestOptions.DEFAULT);
        Map<String, ProfileShardResult> profilingResults =
                response.getProfileResults();
        for (Map.Entry<String, ProfileShardResult> profilingResult : profilingResults.entrySet()) {
            String key = profilingResult.getKey();
            ProfileShardResult profileShardResult = profilingResult.getValue();
            List<QueryProfileShardResult> queryProfileShardResults =
                    profileShardResult.getQueryProfileResults();
            for (QueryProfileShardResult queryProfileResult : queryProfileShardResults) {
                for (ProfileResult profileResult : queryProfileResult.getQueryResults()) {
                    String queryName = profileResult.getQueryName();
                    long queryTimeInMillis = profileResult.getTime();
                    List<ProfileResult> profiledChildren = profileResult.getProfiledChildren();
                    System.out.printf("key = %s, queryName = %s, queryInMillis = %s\n", key, queryName, queryTimeInMillis);
                    profiledChildren.forEach(one-> {
                        String luceneDescription = one.getLuceneDescription();
                        String queryName2 = one.getQueryName();
                        long time = one.getTime();
                        System.out.printf("queryName = %s, luceneDescription = %s, time = %s\n",
                                luceneDescription, queryName2, time);
                    });
                }
            }
        }


        // aggregation with async
        request = new SearchRequest("bank");
        searchSourceBuilder = new SearchSourceBuilder();
        aggregation = AggregationBuilders.terms("group_by_state")
                .field("state.keyword");
        aggregation.subAggregation(AggregationBuilders.avg("average_balance")
                .field("balance"));
        searchSourceBuilder.aggregation(aggregation);
        request.source(searchSourceBuilder);


        final ActionListener<SearchResponse> searchResponseActionListener =
                new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        System.out.println("<<searchAPI [with aggregation]>>");
                        response.getHits().forEach(one-> {
                            System.out.println(one.toString());
                        });

                        Aggregations aggregations = response.getAggregations();
                        Terms terms = aggregations.get("group_by_state");
                        System.out.println("<<All Buckets>>");
                        terms.getBuckets().forEach(one-> {
                            String bucketKey = one.getKey().toString();
                            long docCount = one.getDocCount();
                            Avg averageBalance = one.getAggregations().get("average_balance");
                            double averageBalanceValue = averageBalance.getValue();
                            System.out.printf("key = %s, docCount = %s, averageBalance = %s\n", bucketKey, docCount,
                                    averageBalanceValue);
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                };

        // Throws Error: java.nio.channels.ClosedChannelException
//        client.searchAsync(request, RequestOptions.DEFAULT, searchResponseActionListener);


    }



    private static void updateAPI() throws IOException {
        // first create doc
        indexAPI();

        // now update
        UpdateRequest request = new UpdateRequest(
                "posts",
                "1")
                .doc("updated", new Date(),
                        "reason", "Daily update");

        System.out.println("<<updateAPI()>>");
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        response.getIndex();
        response.getId();
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        System.out.println(response.toString());


        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated 2", new Date());
        jsonMap.put("reason 2", "daily update");
        request = new UpdateRequest("posts", "1")
                .doc(jsonMap);

        response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());

        // does not work
/*        // if doc does not exist, it will be created
        request = new UpdateRequest("posts", "2");
        request.upsert("newly created at", new Date());
        response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());*/
    }

    private static void deleteAPI() throws IOException {
        DeleteRequest request = new DeleteRequest(
                "posts",
                "1");

        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        DeleteResponse deleteResponse = client.delete(
                request, RequestOptions.DEFAULT);

        deleteResponse.getIndex();
        deleteResponse.getId();
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        shardInfo.getTotal();
        shardInfo.getSuccessful();
        shardInfo.getFailed();
        System.out.println(deleteResponse.toString());
    }

    private static void existsAPI() throws IOException {
        GetRequest getRequest = new GetRequest(
                "posts",
                "1");
        // disable fetching source => because exists returns true|false
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

        System.out.printf("Document with {index = posts and id = 1} exists? = %s\n", exists);
        getRequest = new GetRequest(
                "posts",
                "2"
        );
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.printf("Document with {index = posts and id = 2} exists? = %s\n", exists);


        // asynchronous execution
        getRequest = new GetRequest(
                "posts",
                "1");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");


        // async does not work as expected => Connection closed unexpectedly
/*        getRequest.realtime(false);

        client.existsAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                System.out.printf("Document exists = %s\n", aBoolean);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        });*/
    }

    private static void getAPI() throws IOException {
        GetRequest request = new GetRequest(
                "posts",
                "1");

        // include and exclude fields
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        getResponse.getIndex();
        getResponse.getId();
        Map<String, DocumentField> fields= getResponse.getFields();
        getResponse.getPrimaryTerm();
        getResponse.getSeqNo();
        Map<String, Object> source = getResponse.getSource();
        getResponse.getSourceAsString();

        System.out.println("<<getAPI>>");
        System.out.printf("SourceAsString = %s\n", getResponse.getSourceAsString());
        // getFields() => size = 0
        System.out.println(getResponse.getFields().size());
        getResponse.getFields().forEach((key, value) -> {
            System.out.printf("key = %s, document_field = (%s, %s)\n", key, value.getName(), String.join("; ", value.getValues().stream().map(two-> two.toString()).collect(Collectors.toList())));
        });
    }

    private static void indexAPI() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("posts")
                .id("1").source(builder);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        response.getId();
        response.getIndex();
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        shardInfo.getTotal();
        shardInfo.getFailed();
        shardInfo.getSuccessful();
        ReplicationResponse.ShardInfo.Failure[] failures = shardInfo.getFailures();
        System.out.println(response);

    }
}
