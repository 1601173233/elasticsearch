package hyj.elasticsearch.handler;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchHandler {
	public static void main(String[] args) throws UnknownHostException {
		ElasticSearchHandler elasticSearchHandler = new ElasticSearchHandler();
		TransportClient client = elasticSearchHandler.createClient("127.0.0.1", 9300);
		
		QueryBuilder matchQuery = QueryBuilders.matchQuery("title", "编程");
		HighlightBuilder hiBuilder = new HighlightBuilder();
		hiBuilder.preTags("<h2>");
		hiBuilder.postTags("</h2>");
		hiBuilder.field("title");
		
		// 搜索数据
		SearchResponse response = client.prepareSearch("blog")
				.setQuery(matchQuery).highlighter(hiBuilder).execute()
				.actionGet();
		
		// 获取查询结果集
		SearchHits searchHits = response.getHits();
		System.out.println("共搜到:" + searchHits.getTotalHits() + "条结果!");
		// 遍历结果
		for (SearchHit hit : searchHits) {
			System.out.println("String方式打印文档搜索内容:");
			System.out.println(hit.getSourceAsString());
			System.out.println("Map方式打印高亮内容");
			System.out.println(hit.getHighlightFields());

			System.out.println("遍历高亮集合，打印高亮片段:");
			Text[] text = hit.getHighlightFields().get("title").getFragments();
			for (Text str : text) {
				System.out.println(str.string());
			}
		}
	}
	
	/**
	 * 创建连接
	 * @param ip   ip地址
	 * @param port 端口号
	 * @return
	 * @throws UnknownHostException 
	 */
	public TransportClient createClient(String ip, int port) throws UnknownHostException{
		// TODO Auto-generated method stub
		// 设置集群名称
		Settings settings = Settings.builder()
				.put("cluster.name", "elasticsearch").build();

		// 创建client
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress
						.getByName(ip), port));
		
		return client;
	}
}