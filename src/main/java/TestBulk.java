import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.SimpleEvent;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.vf.flume.sink.elasticsearch2.ElasticSearchDynamicSerializer;

public class TestBulk {

	public static void main(String[] args) throws Exception {
		Client client = null;

		ElasticSearchDynamicSerializer serializer = new ElasticSearchDynamicSerializer();

		Settings settings = Settings.settingsBuilder().put("cluster.name", "logstash").build();

		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		transportClient
				.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.65.215.34", 9300)));

		if (client != null) {
			client.close();
		}
		client = transportClient;

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

		IndexRequestBuilder indexRequestBuilder;

		Map<String, String> headers;
		Event event;
		
		while(true){
		long start = System.currentTimeMillis() ;
		for (int count = 0; count < 5000; ++count) {

			event = new SimpleEvent();

			headers = new HashMap<String, String>();
			headers.put("timestamp", "" + System.currentTimeMillis());

			event.setBody(("" + System.currentTimeMillis()).getBytes());
			event.setHeaders(headers);
			;

			indexRequestBuilder = client.prepareIndex("index", "indextype")
					.setSource(serializer.getContentBuilder(event).bytes());
			bulkRequestBuilder.add(indexRequestBuilder);

		}
		
		

		try {
			System.out.println((System.currentTimeMillis()- start) +"              execute size == " + bulkRequestBuilder.numberOfActions());
			;

			BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				throw new EventDeliveryException(bulkResponse.buildFailureMessage());
			}
		} finally {
			bulkRequestBuilder = client.prepareBulk();
			System.out.println((System.currentTimeMillis()- start) +"           init   execute size == " + bulkRequestBuilder.numberOfActions());
			;
		}
		System.out.println( (System.currentTimeMillis()- start)  ) ; 
		Thread.currentThread().sleep(1000L);

		}
		
	}

}
