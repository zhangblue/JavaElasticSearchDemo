package cn.com.zhangd.javaes;

import cn.com.zhangd.es.ElasticSearchManager;
import cn.com.zhangd.utils.FileTools;
import cn.com.zhangd.utils.GuavaFilesTools;
import cn.com.zhangd.utils.LargeLineProcessor;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangdi on 2017/02/20.
 * java 将数据入es
 */

public class JavaToES {

    public static void main(String[] args) {
        JavaToES javaToES = new JavaToES();

        javaToES.insertES();//测试批量出入

        //javaToES.backUpElasticsearchToLocalFile("index_demo", "type_demo", "localFile");//测试备份esindex到本地
        //javaToES.restoreElasticsearchFromLocalFile(10000, "localFile");//测试从文件恢复数据到es
        //javaToES.backUpElasticSearchIndex();//测试导出es的所有index名称

        javaToES.searchElasticSearch();

    }

    /***
     * 批量插入es
     */
    public void insertES() {
        ElasticSearchManager elasticSearchManager = new ElasticSearchManager();

        try {
            //建立es连接
            elasticSearchManager.initElasticSearch("wfpt_es", "192.168.10.16", 9300);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        BulkProcessor bulkProcessor = elasticSearchManager.getBulk();


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

    /**
     * 将es中的表备份到本地文件
     *
     * @param _index   es index名字
     * @param _type    es type名字
     * @param filePath 备份到本地的文件
     */
    public void backUpElasticsearchToLocalFile(String _index, String _type, String filePath) {
        //设置文件操作工具对象
        FileTools fileUtil = new FileTools();

        //初始化es连接
        ElasticSearchManager elasticSearchManager = new ElasticSearchManager();
        try {
            //建立es连接
            elasticSearchManager.initElasticSearch("wfpt_es", "192.168.10.16", 9300);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        //设施查询参数
        SearchResponse response = elasticSearchManager.getESClient().prepareSearch(_index).setTypes(_type)
                .setQuery(QueryBuilders.matchAllQuery()).setSize(50000).setScroll(new TimeValue(600000))
                .setSearchType(SearchType.SCAN).execute().actionGet();
        //设置游标
        String strScrollId = response.getScrollId();

        while (true) {
            SearchResponse searchResponse = elasticSearchManager.getESClient().prepareSearchScroll(strScrollId)
                    .setScroll(new TimeValue(1000000)).execute().actionGet();
            SearchHits searchHit1 = searchResponse.getHits();
            // 再次查询不到数据时跳出循环
            if (searchHit1.getHits().length == 0) {
                break;
            } else {
                //设置存储容器，每50000行写一次文件
                Vector<String> vector = new Vector<String>(50000);
                Map<String, Object> mapSource = null;
                SearchHit[] searchHits = searchHit1.hits();

                for (SearchHit searchHit : searchHits) {
                    mapSource = searchHit.getSource();
                    mapSource.put("_index", searchHit.getIndex());
                    mapSource.put("_type", searchHit.getType());
                    mapSource.put("_id", searchHit.getId());
                    vector.add(JSONObject.toJSONString(mapSource));
                }
                System.out.println("list sie = " + vector.size());
                //写文件，使用追加方式
                fileUtil.appendContentToFile(filePath, vector);
                vector.clear();
            }
        }
        //关闭ES连接
        elasticSearchManager.closeElasticSearchConn();
    }


    /**
     * 将备份的文件恢复到es中
     *
     * @param filePath       备份的本地文件名
     * @param fileLineNumber 文件行数
     */
    public void restoreElasticsearchFromLocalFile(long fileLineNumber, String filePath) {
        FileTools fileUtil = new FileTools();

        //初始化es连接
        ElasticSearchManager elasticSearchManager = new ElasticSearchManager();
        try {
            //建立es连接
            elasticSearchManager.initElasticSearch("wfpt_es", "192.168.10.16", 9300);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        //设置es批处理对象
        BulkProcessor bulkProcessor = elasticSearchManager.getBulk();

        int limit_size = 50000;
        //通过文件总行数与每次读取行数来计算出一共需要读几次文件才可把文件读完
        long size = fileLineNumber % limit_size == 0 ? fileLineNumber / limit_size : fileLineNumber / limit_size + 1;
        long begin_line = 0;
        List<String> list = null;
        JSONObject js = null;

        IndexRequest indexRequest = null;
        UpdateRequest updateRequest = null;
        for (int i = 0; i < size; i++) {
            list = fileUtil.readFileByLinesBeginToEnd(filePath, begin_line + 1, limit_size);
            String indexvalue = null;  //index名
            String typevalue = null; //type名
            String idvalue = null; //id名

            for (String strLine : list) {
                js = JSONObject.parseObject(strLine);
                indexvalue = js.get("_index").toString();
                typevalue = js.get("_type").toString();
                idvalue = js.get("_id").toString();
                //这里因为备份文件中包含_index、_type、_id，并且入库时不需要入到source中。所以此处将这三部分内容去掉
                js.remove("_index");
                js.remove("_id");
                js.remove("_type");

                indexRequest = new IndexRequest(indexvalue, typevalue, idvalue).source(js);
                updateRequest = new UpdateRequest(indexvalue, typevalue, idvalue).doc(js)
                        .upsert(indexRequest);
                bulkProcessor.add(updateRequest);
            }
            System.out.println("finish =" + list.size());
            begin_line = begin_line + limit_size;
        }

        try {
            //等待15秒后关闭批处理对象。因为批处理对象设置为每5秒钟刷新一次es，这里如果不等待而直接关闭会导致结尾部分数据没有入到es中
            bulkProcessor.awaitClose(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //关闭es连接
        elasticSearchManager.closeElasticSearchConn();
    }

    /***
     * 得到es的所有index
     */
    public void backUpElasticSearchIndex() {
        FileTools fileUtil = new FileTools();

        //初始化es连接
        ElasticSearchManager elasticSearchManager = new ElasticSearchManager();
        try {
            //建立es连接
            elasticSearchManager.initElasticSearch("wfpt_es", "192.168.10.16", 9300);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        String[] indexies = elasticSearchManager.getESClient().admin().indices().prepareGetIndex().get().getIndices();

        List<String> listIndex = Arrays.asList(indexies);
        fileUtil.appendContentToFile("/home/hdfs/zhangd/elasticsearch_test/data/indexies", listIndex);
    }

    public void searchElasticSearch() {
        GuavaFilesTools gft = new GuavaFilesTools();
        //初始化es连接
        ElasticSearchManager elasticSearchManager = new ElasticSearchManager();
        try {
            //建立es连接
            elasticSearchManager.initElasticSearch("hanyingjun", "192.168.10.15", 9300);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        LargeLineProcessor largeLineProcessor = gft.readLargeFile("/home/hdfs/zhangd/test_ids", 1, 300000);

        List<String> list = largeLineProcessor.getResult();

        long begin = Calendar.getInstance().getTimeInMillis();
        String[] straa = null;
        Vector<String> ve = new Vector<String>(300000);
        int size = 0;
        try {
            for (String str : list) {
                straa = str.split("\t");
                GetResponse response = elasticSearchManager.getESClient().prepareGet("track_hyj", "track", straa[0]).get();
                ve.add(JSONObject.toJSON(response.getSource()).toString());
                if (ve.size() % 5000 == 0) {
                    System.out.println(ve.size());
                }
                size++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(list.get(size));
        }finally {
            elasticSearchManager.closeElasticSearchConn();
        }

        long end = Calendar.getInstance().getTimeInMillis();
        System.out.println((end - begin) / 1000);
    }
}
