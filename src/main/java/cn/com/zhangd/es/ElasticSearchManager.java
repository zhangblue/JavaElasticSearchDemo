package cn.com.zhangd.es;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by zhangdi on 2017/02/20.
 */
public class ElasticSearchManager {

    private Client client;

    /***
     * elasticsearch 初始化
     * @param clusterName es集群的名称
     * @param clusterHost es集群的host
     * @param clusterPort es集群的端口
     * @throws UnknownHostException
     */
    public void initElasticSearch(String clusterName, String clusterHost, int clusterPort) throws UnknownHostException {
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).build();
        client = TransportClient.builder().settings(settings).build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName(clusterHost), clusterPort));
    }


    /***
     * 获取es client对象
     * @return
     */
    public Client getESClient() {
        return client;
    }


    /***
     * 获取批量入库对象
     * @param client
     * @return
     */
    public BulkProcessor getBulk(Client client) {
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                    }

                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                    }

                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                    }
                })
                .setBulkActions(5000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))//设置提交内容大小
                .setFlushInterval(TimeValue.timeValueSeconds(5))//设置刷新时间
                .setConcurrentRequests(3)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        return bulkProcessor;
    }

    /***
     * 关闭es连接
     */
    public void closeElasticSearchConn() {
        client.close();
    }
}
