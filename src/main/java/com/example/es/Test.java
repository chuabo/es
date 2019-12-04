package com.example.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/test")
public class Test {

    //HighLevelClient highlightLevelClient;
    //@Autowired
    //ElasticsearchRestClient client;
    //ElasticsearchRestClient client=new ElasticsearchRestClient();
    //RestHighLevelClient highLevelClient=client.highLevelClient(client.restClientBuilder());

    //RestHighLevelClient highLevelClient=new RestHighLevelClient(

    //)
    //RestHighLevelClient highLevelClient=null;
    RestHighLevelClient highLevelClient = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("192.168.137.132", 9200, "http"),
                    new HttpHost("192.168.137.132", 9201, "http")));

    private void queryBuilder(Integer pageIndex,Integer pageSize,Map<String,Object>query,String indexName,SearchRequest searchRequest){
        if(query!=null && !query.keySet().isEmpty()){
            SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
            if(pageIndex!=null && pageSize!=null){
                searchSourceBuilder.size(pageSize);
                if(pageIndex<=0){
                    pageIndex=0;
                }
                searchSourceBuilder.from((pageIndex -1)*pageSize);

            }
            BoolQueryBuilder boolQueryBuilder= QueryBuilders.boolQuery();
            query.keySet().forEach(key->{
                boolQueryBuilder.must(QueryBuilders.matchQuery(key,query.get(key)));
            });
            searchSourceBuilder.query(boolQueryBuilder);

            HighlightBuilder highlighBuilder=new HighlightBuilder();
            HighlightBuilder.Field hightlightTitle=new HighlightBuilder.Field("applyId").preTags("<strong>").postTags("</strong>");
            hightlightTitle.highlighterType("unified");
            highlighBuilder.field(hightlightTitle);
            searchSourceBuilder.highlighter(highlighBuilder);
            SearchRequest source = searchRequest.source(searchSourceBuilder);
        }
    }

    @RequestMapping(value = "/test",method = RequestMethod.GET)
    @ResponseBody
    public List<Map<String,Object>> test(@RequestParam String keyword){
        Integer pageIndex=1;
        Integer pageSize=5;
        String indexName="mining_search";
        Map<String,Object> data=new HashMap();
        data.put("applyId",keyword);
        data.put("offerId",keyword);

        List<Map<String,Object>> result=new ArrayList();
        SearchRequest searchRequest=new SearchRequest(indexName);
        queryBuilder(pageIndex,pageSize,data,indexName,searchRequest);
        try{
            SearchResponse response=highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            for(SearchHit hit:response.getHits().getHits()){
                Map<String,Object>map=hit.getSourceAsMap();
                map.put("id",hit.getId());
                result.add(map);

                Map<String, HighlightField>highlightFields=hit.getHighlightFields();
                HighlightField highlightField=highlightFields.get("applyId");
                Text[] fragments=highlightField.fragments();
                String fragmentString=fragments[0].string();
                System.out.println("hightlight:"+fragmentString);
            }

            System.out.println("pageIndex:"+pageIndex);
            System.out.println("pageSize:"+pageSize);
            System.out.println(response.getHits().getTotalHits());
            System.out.println(result.size());
            for(Map<String,Object>map:result){
                System.out.println(map.get("name"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

}
