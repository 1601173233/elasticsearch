package hyj.elasticsearch.index;

import hyj.elasticsearch.util.ElasticUtil;
import hyj.elasticsearch.util.XContentUtil;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * 
 * @Description: ES索引操作类
 * @author hyj     
 * @date 2018年1月9日 下午9:23:04  
 * @version V1.0
 */
public class ElasticIndex {
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, Map<String, Object> map) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(map).get();
		
		client.close();
	}
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, String id, Map<String, Object> map) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type, id)
					.setSource(map).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, String jsonDate) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(jsonDate).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param jsonDate 
	 * @throws UnknownHostException 
	 */
	public static void createIndex(String index, String type, String id, String jsonDate) throws UnknownHostException {
		TransportClient client = ElasticUtil.createClient();
		
		IndexResponse response = client.prepareIndex(index, type, id)
					.setSource(jsonDate).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param obj      对象 
	 * @throws UnknownHostException 
	 * @throws JsonProcessingException 
	 */
	public static void createIndex(String index, String type, Object obj) throws UnknownHostException, JsonProcessingException {
		TransportClient client = ElasticUtil.createClient();
		
		// instance a json mapper
		ObjectMapper mapper = new ObjectMapper(); // create once, re
		
		// generate json
		byte[] json = mapper.writeValueAsBytes(obj);
		
		IndexResponse response = client.prepareIndex(index, type)
					.setSource(json).get();
		
		client.close();
	}
	
	/**
	 * 创建索引
	 * @param index    索引名字
	 * @param type     参数类型
	 * @param obj      对象 
	 * @throws UnknownHostException 
	 * @throws JsonProcessingException 
	 */
	public static void createIndex(String index, String type, String id, Object obj) throws UnknownHostException, JsonProcessingException {
		TransportClient client = ElasticUtil.createClient();
		
		// instance a json mapper
		ObjectMapper mapper = new ObjectMapper(); // create once, re
		
		// generate json
		byte[] json = mapper.writeValueAsBytes(obj);
		
		IndexResponse response = client.prepareIndex(index, type, id)
					.setSource(json).get();
		
		client.close();
	}

	/**
	 * 更新索引
	 * 
	 * @param index
	 *            索引名字
	 * @param type
	 *            参数类型
	 * @param id
	 *            索引的id
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void update(String index, String type, String id,
			Map<String, Object> map) throws IOException, InterruptedException,
			ExecutionException {
		TransportClient client = ElasticUtil.createClient();

		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(index);
		updateRequest.type(type);
		updateRequest.id(id);

		// 构造参数列
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		builder = XContentUtil.addFields(map, builder);
		builder = builder.endObject();

		updateRequest.doc(builder);
		client.update(updateRequest).get();
		client.close();
	}

	/**
	 * 删除索引
	 * 
	 * @param index
	 *            索引名字
	 * @param type
	 *            参数类型
	 * @param id
	 *            索引的id
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void delete(String index, String type, String id)
			throws IOException, InterruptedException, ExecutionException {
		TransportClient client = ElasticUtil.createClient();

		DeleteResponse response = client.prepareDelete(index, type, id).get();
		client.close();
	}

	/**
	 * 根据条件删除索引
	 * 
	 * @param index
	 *            索引名字
	 * @param type
	 *            参数类型
	 * @param id
	 *            索引的id
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void deleteByQuery(String index, String key, String value)
			throws IOException, InterruptedException, ExecutionException {
		TransportClient client = ElasticUtil.createClient();

		BulkIndexByScrollResponse response = DeleteByQueryAction.INSTANCE
				.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery(key, value)) // 查询条件
				.source(index) // index(索引名)
				.get(); // 执行
		
		long deleted = response.getDeleted(); // 删除文档的数量
		client.close();
	}

	/**
	 * 根据条件删除索引
	 * 
	 * @param index
	 *            索引名字
	 * @param type
	 *            参数类型
	 * @param id
	 *            索引的id
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void deleteByQuery(String index, String key, String value, ActionListener<BulkIndexByScrollResponse> actionListener)
			throws IOException, InterruptedException, ExecutionException {
		TransportClient client = ElasticUtil.createClient();

		DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery(key, value)) // 查询条件
				.source(index) // index(索引名)
				.execute(actionListener); // 执行
		client.close();
	}
	
	/**
	 * 批量操作
	 * @return 
	 * @throws UnknownHostException 
	 */
	public static BulkResponse blukRequest(List<ActionRequest> actionRequestList) throws UnknownHostException{
		TransportClient client = ElasticUtil.createClient();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		for(ActionRequest actionRequest : actionRequestList){
			if(actionRequest instanceof IndexRequest){
				bulkRequest.add((IndexRequest) actionRequest);
			}else if(actionRequest instanceof UpdateRequest){
				bulkRequest.add((UpdateRequest) actionRequest);
			}else if(actionRequest instanceof DeleteRequest){
				bulkRequest.add((DeleteRequest) actionRequest);
			}
		}
		
		BulkResponse bulkResponse = bulkRequest.get();
		
		client.close();
		return bulkResponse;
	}
	
	public static void main(String[] args) throws UnknownHostException {
		String json = "{" +
				"\"id\":\"1\"," +
				"\"user\":\"hyj1\"," +
				"\"postDate\":\"2018-01-30\"," +
				"\"message\":\"trying out Elasticsearch\"" +
				"}";
		
		createIndex("text", "text", "1", json);

	}

}