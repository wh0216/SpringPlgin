package com.wh.es;

import com.alibaba.fastjson.JSON;
import com.wh.es.entity.ArchitectureDto;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringDataElasticSearchMainApplication.class)
public class EsHandTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 测试文档的添加
    @Test
    public void testCreateDoc() throws IOException {
        //CreateDoc5()创建实体方法，可自己实现
        ArchitectureDto architectureDto = new ArchitectureDto();
        architectureDto.setAddress("昌平区");
        architectureDto.setArea("亚洲");
        architectureDto.setCity("北京");
        architectureDto.setDescription("测试");
        architectureDto.setId("2");
        architectureDto.setName("王鸿哈哈哈今天吉萨擦拭舒服噶回复大花撒u大u");
        architectureDto.setPrice(10);
        // 创建好index请求
        IndexRequest indexRequest = new IndexRequest("architecture_index");
        // 设置索引
        indexRequest.id("5");
        // 设置超时时间（默认）
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        // 往请求中添加数据

        indexRequest.source(JSON.toJSONString(architectureDto), XContentType.JSON);
        //执行添加请求
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }

    @Test
    public void getDoc() throws IOException {
        //获得查询索引的请求对象
        GetRequest gerRequest = new GetRequest("architecture_index").id("5");
        //获得文档对象
        GetResponse doc = restHighLevelClient.get(gerRequest, RequestOptions.DEFAULT);
        //获得文档数据
        System.out.println(doc.getSourceAsString());
    }

    @Test
    public void delDoc() throws IOException {
        //获得删除的索引请求对象
        DeleteRequest delRequest = new DeleteRequest("architecture_index").id("1");
        //删除文档
        DeleteResponse delete = restHighLevelClient.delete(delRequest, RequestOptions.DEFAULT);
        System.out.println(delete.getIndex());
    }

    @Test
    public void delIndex() throws IOException {
        IndicesClient indices = restHighLevelClient.indices();
        DeleteIndexRequest delReq = new DeleteIndexRequest("architecture_index");
        AcknowledgedResponse delete = indices.delete(delReq, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    @Test
    public void contextLoads() throws IOException {
        //查询mysql中所有数据
        List<ArchitectureDto> architectures = new ArrayList<>();

        //创建批量处理对象
        BulkRequest bulkRequest = new BulkRequest();

        //循环添加新增处理请求
        for (ArchitectureDto architecture : architectures) {
            String architecturJson = JSON.toJSONString(architecture);
            IndexRequest indexRequest = new IndexRequest("architecture_index").id(architecture.getId() + "").source(architecturJson, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        //提交批量处理对象
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        //查看添加状态
        System.out.println(bulk.status());

    }

 }