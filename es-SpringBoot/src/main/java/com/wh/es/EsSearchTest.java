package com.wh.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wh.es.entity.ArchitectureDto;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;


import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


import java.io.IOException;
import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringDataElasticSearchMainApplication.class)
public class EsSearchTest {

    //https://blog.csdn.net/Oaklkm/article/details/125896279?spm=1018.2226.3001.4187
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    /**
     * 查询条件数据    匹配查询不到将字段类似设置为.keyword
     * @throws IOException
     */
    @Test
    public void searchAll() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        //制定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());   //查询所有
//        builder.query(QueryBuilders.termQuery("address","huahexi777"));    //非String 类型查询
//        builder.query(QueryBuilders.termQuery("location.lat",33.2));    //非String 类型查询
//        builder.query(QueryBuilders.matchPhraseQuery("area","高新区"));   //精准查询String
//        builder.query(QueryBuilders.matchQuery("area.keyword","高新区"));   // 不能匹配到
//        builder.query(QueryBuilders.termQuery("area.keyword","高新区"));
//        builder.sort("price", SortOrder.DESC);
        request.source(builder);
        //获得文档对象
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        //获得文档数据
        for (SearchHit hit : search.getHits().getHits()) {
            ArchitectureDto art = JSONObject.parseObject(hit.getSourceAsString(), ArchitectureDto.class);
            System.out.println(JSON.toJSONString(art));
        }
    }

    /**
     * 类似于数据库的 or 查询
     * @throws IOException
     */
    @Test
    public void searchByBolt() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        //制定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.matchQuery("price",100));
       // boolQueryBuilder.should(QueryBuilders.matchQuery("score",4.6).boost(10));
        builder.query(boolQueryBuilder);    //非String 类型查询
        request.source(builder);
        //获得文档对象
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        //获得文档数据
        for (SearchHit hit : search.getHits().getHits()) {
            ArchitectureDto art = JSONObject.parseObject(hit.getSourceAsString(), ArchitectureDto.class);
            System.out.println(JSON.toJSONString(art));
        }
    }

    /**
     * 查询部分字段
     * @throws IOException
     */
    @Test
    public void searchByParam() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        //制定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());   //查询所有
        String[]  includes = {"name","address","price"};
        String[] excludes = {};
        /** 会多出一个score字段 默认值都为0 具体原因不详  */
        builder.fetchSource(includes,excludes);
        request.source(builder);
        //获得文档对象
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        //获得文档数据
        for (SearchHit hit : search.getHits().getHits()) {
            ArchitectureDto art = JSONObject.parseObject(hit.getSourceAsString(), ArchitectureDto.class);
            System.out.println(JSON.toJSONString(art));
        }
    }

    /**
     * 范围查询 大于小于
     * @throws IOException
     */
    @Test
    public void searchByFilter() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        //制定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //定制查询条件
        boolQueryBuilder.filter (QueryBuilders.rangeQuery("price").gte(10).lte(100));
        builder.query(boolQueryBuilder);    //非String 类型查询
        request.source(builder);

        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : search.getHits().getHits()) {
            ArchitectureDto art = JSONObject.parseObject(hit.getSourceAsString(), ArchitectureDto.class);
            System.out.println(JSON.toJSONString(art));
        }
    }

    /**
     * 迷糊查询
     * @throws IOException
     */
//    @Test
//    public void searchByLike() throws IOException {
//        //定义请求对象
//        SearchRequest request = new SearchRequest("architecture_index");
//        //制定检索条件
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        //定制查询条件
//        TermQueryBuilder builder = QueryBuilders.termQuery("name.keyword","北京大").queryName(Fuzziness.ONE));
//searchSourceBuilder.query(builder);
//        request.source(searchSourceBuilder);
//
//        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
//        for (SearchHit hit : search.getHits().getHits()) {
//            ArchitectureDto art = JSONObject.parseObject(hit.getSourceAsString(), ArchitectureDto.class);
//            System.out.println(JSON.toJSONString(art));
//        }
//    }


    /**
     * 聚合查询所有景点门票和
     * @throws IOException
     */
    @Test
    public void searchSum() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //制定检索条件
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.sum("sum_price").field("price"));
        //组装
        request.source(sourceBuilder);
        //执行
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        //打印数据
        Aggregations aggregations = response.getAggregations();
        System.out.println(JSON.toJSONString(aggregations.getAsMap().get("sum_price")));
    }
    /**
     * 聚合查询所有景点总数
     * @throws IOException
     */
    @Test
    public void searchCount() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //制定检索条件
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.count("count_name").field("name.keyword"));
        //组装
        request.source(sourceBuilder);
        //执行
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        //打印数据
        Aggregations aggregations = response.getAggregations();
        System.out.println(JSON.toJSONString(aggregations.getAsMap().get("count_name")));
    }
    /**
     * 查询门票价格 最大值，最小值，平均值等
     * @throws IOException
     */
    @Test
    public void searchStats() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //制定检索条件
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.stats("status_price").field("price"));
        //组装
        request.source(sourceBuilder);
        //执行
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        //打印数据
        Aggregations aggregations = response.getAggregations();
        System.out.println(JSON.toJSONString(aggregations.getAsMap().get("status_price")));
    }
    /**
     * 求和 以及求平均值
     * @throws IOException
     */
    @Test
    public void search2() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //制定检索条件
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.count("count_name").field("name.keyword"))
                .aggregation(AggregationBuilders.avg("avg_price").field("price"));
        //组装
        request.source(sourceBuilder);
        //执行
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        //打印数据
        Aggregations aggregations = response.getAggregations();
        System.out.println(JSON.toJSONString(aggregations.getAsMap().get("count_name")));
        System.out.println(JSON.toJSONString(aggregations.getAsMap().get("avg_price")));
    }
    /**
     * 统计价格在0-30  30-60  60-100不同阶段的景点数量
     * @throws IOException
     */
    @Test
    public void search1() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //制定检索条件
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range("price_range")
                .field("price")
                .addRange(0,30)
                .addRange(30,60)
                .addRange(60,100);

        //组装
        sourceBuilder.aggregation(rangeAggregationBuilder);
        request.source(sourceBuilder);
        //执行
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        //打印数据
        Aggregations aggregations = response.getAggregations();
        Aggregation aggregation = aggregations.get("price_range");
        System.out.println(JSON.toJSONString(aggregation));
        //获取桶聚合结果
        List<? extends Range.Bucket> buckets = ((Range) aggregation).getBuckets();
        //循环遍历各个桶结果
        for (Range.Bucket bucket : buckets) {
            //分组的key
            String key = bucket.getKeyAsString();
            //分组的值
            long docCount = bucket.getDocCount();
            System.out.println(key + "------->" + docCount);
        }
    }

    /**
     * 查询每个城市景点数量 总价格 平均价格
     * @throws IOException
     */
    @Test
    public void search3() throws IOException {
        //定义请求对象
        SearchRequest request = new SearchRequest("architecture_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //制定检索条件
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders
                .terms("city_group").field("city.keyword")
                .subAggregation(AggregationBuilders.sum("sum_price").field("price"))
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"));


        //组装
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.aggregation(termsAggregationBuilder);
        sourceBuilder.size(0);
        request.source(sourceBuilder);
//        System.out.println("dsl:" + sourceBuilder.toString());
        //执行
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        listAggregations(response);
    }


    public static void listAggregations(SearchResponse searchResponse) {
        // 处理聚合查询结果
        Aggregations aggregations = searchResponse.getAggregations();
        Terms byShopAggregation = aggregations.get("city_group");

        // 遍历terms聚合结果
        for (Terms.Bucket bucket : byShopAggregation.getBuckets()) {
            String color = bucket.getKeyAsString();
            long colorCount = bucket.getDocCount();
            System.out.print("城市 " + color + "\n数量 : " + colorCount);

            // 根据avg_price聚合名字，获取嵌套聚合结果
            Sum sum = bucket.getAggregations().get("sum_price");
            // 获取平均价格
            double sumPrice = sum.getValue();
            System.out.print(" ; 总价格 : " + sumPrice);

            // 根据avg_price聚合名字，获取嵌套聚合结果
            Avg avg = bucket.getAggregations().get("avg_price");
            // 获取平均价格
            double avgPrice = avg.getValue();
            System.out.println(" ; 平均价格 : " + avgPrice);
        }
    }
    /**
     * 浅分页查询
     *
     * @throws IOException
     */
    @Test
    public  void testPage() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //排序条件
       // searchSourceBuilder.sort("id", SortOrder.ASC);
        //searchSourceBuilder.sort("publishTime", SortOrder.DESC);
        //分页查询
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(2);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search.toString());
    }
//scroll的RestHighLevelClient实现
@Test
    public  void testScroll() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        //失效时间为1min
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1));
        //封存快照
        searchRequest.scroll(scroll);
        /**
         * 查询条件
         */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("tag", "疫情");
//        searchSourceBuilder.query(termQueryBuilder);
        /**
         * 分页参数
         */
        searchSourceBuilder.size(2);
        searchRequest.indices("architecture_index");

        //放入文档中
        searchRequest.source(searchSourceBuilder);
    System.out.println("dsl:" + searchSourceBuilder.toString());
        //远程查询
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //元素数量

        Iterator<SearchHit> it1 = searchResponse.getHits().iterator();
        while (it1.hasNext()) {
            SearchHit next = it1.next();
            System.out.println("输出数据:" + next.getSourceAsString());
        }
        System.out.println("=======================||");
        //计算总数量
        long totalCount = searchResponse.getHits().getTotalHits().value;
        //得到总页数
        int page = (int) Math.ceil((float) totalCount / 2);
        //多次遍历分页，获取结果
        String scrollId = searchResponse.getScrollId();
        for (int i = 1; i <= page; i++) {
            //获取到该id
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);
            SearchResponse response = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            //打印数据
            SearchHits hits = response.getHits();
            scrollId = response.getScrollId();
            Iterator<SearchHit> iterator = hits.iterator();
            while (iterator.hasNext()) {
                SearchHit next = iterator.next();
                System.out.println("输出数据:" + next.getSourceAsString());
            }
            System.out.println("=======================");
        }
    }


    /**
     * 高亮查询
     */
    @Test
    public void testHighlight() throws IOException {
        // 高亮查询
        SearchRequest request = new SearchRequest().indices("architecture_index1");
//2.创建查询请求体构建器
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//构建查询方式：高亮查询TermsQueryBuilder termsQueryBuilder =
        //QueryBuilders.termsQuery("address","昌平区");
//设置查询方式sourceBuilder.query(termsQueryBuilder);
        //构建高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder(); highlightBuilder.preTags("<font color='red'>");//设置标签前缀highlightBuilder.postTags("</font>");//设置标签后缀highlightBuilder.field("name");//设置高亮字段
//设置高亮构建对象sourceBuilder.highlighter(highlightBuilder);
//设置请求体
        request.source(sourceBuilder);
//3.客户端发送请求，获取响应对象
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

//4.打印响应结果
        SearchHits hits = response.getHits(); System.out.println("took::"+response.getTook()); System.out.println("time_out::"+response.isTimedOut()); System.out.println("total::"+hits.getTotalHits()); System.out.println("max_score::"+hits.getMaxScore()); System.out.println("hits::::>>");
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString(); System.out.println(sourceAsString);
//打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields+"=====");
        }
        System.out.println("<<::::");
    }


    /**
     * index :hotel
     *
     * @throws IOException
     */
    @Test
    public void serchPageBuilder() throws IOException {

        //条件搜索
        //1、构建搜索请求
        SearchRequest jd_goods = new SearchRequest("architecture_index");
        //2、设置搜索条件，使用该构建器进行查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建精确匹配查询条件
//        TermQueryBuilder termQuery = QueryBuilders.termQuery("title", keyword);
//        TermQueryBuilder termQuery = QueryBuilders.termQuery("title.keyword", keyword);
//        searchSourceBuilder.query(termQuery);




        // 自试 索引查询
//        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
//        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "哈哈");   // 成功【单子多字都成功】
//        searchSourceBuilder.query(queryBuilder);
//        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "轻松");   // 成功【单子多字都成功】
//        searchSourceBuilder.query(queryBuilder);



//        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
//        searchSourceBuilder.from(1);
//        searchSourceBuilder.size(9);


//        HighlightBuilder.Field field = new HighlightBuilder.Field("title");
//        field.preTags("<font color='red'>");
//        field.postTags("</font>");
//        field.fragmentSize(100);






        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(true);
        highlightBuilder.field("name");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        // highlightBuilder.requireFieldMatch(false);//匹配第一个即可
        // highlightBuilder.numOfFragments(0);

        searchSourceBuilder.highlighter(highlightBuilder);
        //3、将搜索条件放入搜索请求中
        jd_goods.source(searchSourceBuilder);
        //4、客户端执行搜索请求
        SearchResponse search = restHighLevelClient.search(jd_goods, RequestOptions.DEFAULT);
        System.out.println("共查询到"+search.getHits().getHits().length+"条数据");

        //5、打印测试
        Map<String, Object> map = new HashMap<>();
        ArrayList<Map<String,Object>> list = new ArrayList();
        for (SearchHit hit : search.getHits()) {

            //

            String value = hit.getSourceAsString();
            ArchitectureDto esProductTO = JSON.parseObject(value, ArchitectureDto.class);

            map.put("fragment", JSON.toJSONString(esProductTO.getName()));
            System.out.println(esProductTO.getName());


            map.put("fr", JSON.toJSONString(esProductTO.getName()));


            map = hit.getSourceAsMap();
            //System.out.println("hit = " + hit);
            list.add(map);

//            System.out.println("hit：\n" + hit);
//            System.out.println(hit.getHighlightFields() == null);
//            System.out.println(hit.getHighlightFields().get("title"));
//            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
//            System.out.println("highlightFields = " + highlightFields);
//            HighlightField title_high = highlightFields.get("title");
//            String fr = "";
//            for (Text fragment : title_high.fragments()) {
//                System.out.println("fragment = " + fragment);
//                fr = fragment.toString();
//                map.put("fragment", JSON.toJSONString(fragment.toString()));
//
//            }
//            System.out.println("fr = " + fr);
//            // map.put("fragment", JSON.toJSONString(fragment));
//            map.put("fr", JSON.toJSONString(fr));
//            //System.out.println("title_high_______fragments = " + title_high.fragments().toString());
//            map = hit.getSourceAsMap();
//            //System.out.println("hit = " + hit);
//            list.add(map);
        }
        System.out.println(list+"=====");
    }


}


