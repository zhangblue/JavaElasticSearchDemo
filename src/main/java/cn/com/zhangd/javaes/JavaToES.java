package cn.com.zhangd.javaes;

import cn.com.zhangd.es.ElasticSearchManager;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

import java.net.UnknownHostException;
import java.util.HashMap;

/**
 * Created by zhangdi on 2017/02/20.
 * java 将数据入es
 */

public class JavaToES {


    public static void main(String[] args) {
        JavaToES javaToES = new JavaToES();
        javaToES.insertES();
    }

    public void insertES() {
        ElasticSearchManager elasticSearchManager = new ElasticSearchManager();

        try {
            //建立es连接
            elasticSearchManager.initElasticSearch("wfpt_es", "192.168.10.16", 9300);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        BulkProcessor bulkProcessor = elasticSearchManager.getBulk(elasticSearchManager.getESClient());


        HashMap<String, Object> hashMap = new HashMap<String, Object>();

        hashMap.put("name", "张三");
        hashMap.put("age", 10);
        hashMap.put("sex", "男");

        //入库到index type id，如果es中没有则会自动创建。
        IndexRequest indexRequest = new IndexRequest("zhangd_user_index", "user_type", "1").source(hashMap);

        bulkProcessor.add(indexRequest);

        bulkProcessor.flush();

        elasticSearchManager.closeElasticSearchConn();
    }

}
