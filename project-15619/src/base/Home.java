package base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.io.IoCallback;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;

public class Home {
	private final static int NUM_CONNECTIONS = 90;
	private static HTablePool pool;
	private static HTableInterface q2;
	private static HTableInterface q3;
	private static HTableInterface q4;
	private static HTableInterface q5;
	private static HTableInterface q6;

	public Home() {
		try {
	        pool = new HTablePool(HBaseConfiguration.create(), NUM_CONNECTIONS);
	        q2 = pool.getTable(Bytes.toBytes("q2"));
	        q3 = pool.getTable(Bytes.toBytes("q3"));
	        q4 = pool.getTable(Bytes.toBytes("q4"));
	        q5 = pool.getTable(Bytes.toBytes("q5"));
	        q6 = pool.getTable(Bytes.toBytes("q6"));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getQ2(String userid, String timestamp) {		
		Get g = new Get(Bytes.toBytes(userid));
		Result r = null;
		try {
			r = q2.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] value = r.getValue(Bytes.toBytes("t"), Bytes.toBytes(""));
		return Bytes.toString(value).replaceAll("_", "\n"); 
	}

	public String getQ3(String userid) {
		Get g = new Get(Bytes.toBytes(userid));
		Result r = null;
		try {
			r = q3.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] value = r.getValue(Bytes.toBytes("r"), Bytes.toBytes(""));
		return Bytes.toString(value).replaceAll("_", "\n"); 
	}
	
	public String getQ4(String time) {
		Get g = new Get(Bytes.toBytes(time));
		Result r = null;
		try {
			r = q4.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] value = r.getValue(Bytes.toBytes("t"), Bytes.toBytes(""));
		return Bytes.toString(value);
	}
	
	public String getQ5(String start, String end) {
		Scan scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(end));
		ResultScanner r = null;
		Result res = null;
		String result = null;
		try {
			r = q5.getScanner(scan);
			while ((res = r.next()) != null) {
				result += ByteBuffer.wrap(res.getValue(Bytes.toBytes("t"), Bytes.toBytes("")));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result.replaceAll("_","\n");
	}
	
	public String getQ6(String min, String max) {
		long sum = 0L;
		Scan scan = new Scan(Bytes.toBytes(min), Bytes.toBytes(max));
		ResultScanner r = null;
		Result res = null;
		try {
			r = q6.getScanner(scan);
			while ((res = r.next()) != null) {
				sum += ByteBuffer.wrap(res.getValue(Bytes.toBytes("c"), Bytes.toBytes(""))).getLong();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ""+sum;
	}

	public static void main(String[] args) {
		final String info = "TeamSYC,8635-0832-4410\n";
		final SimpleDateFormat fmt = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
		final Charset utf8 = Charset.forName("UTF-8");
		final Home home = new Home();

		Undertow.builder()
		.setWorkerThreads(4096)
		.setIoThreads(Runtime.getRuntime().availableProcessors() * 2)
		.setServerOption(UndertowOptions.ALWAYS_SET_KEEP_ALIVE, false)
		.setBufferSize(1024*16)
		.addHttpListener(80, args[0])
		.setHandler(new HttpHandler() {

			public void handleRequest(final HttpServerExchange exchange) throws Exception {
				char path = exchange.getRequestPath().charAt(2);
				String result = null;
				Map<String,Deque<String>> queryMap = null;
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
				
				switch (path) {
				case '1':
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(fmt.format(new Date())).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
					
				case '2':
					queryMap = exchange.getQueryParameters();
					result = home.getQ2(queryMap.get("userid").getFirst().trim(), 
							queryMap.get("tweet_time").getFirst().trim().replaceAll(" ", "+"));
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
					
				case '3':
					queryMap = exchange.getQueryParameters();
					result = home.getQ3(queryMap.get("userid").getFirst().trim());
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
					
				case '4':
					queryMap = exchange.getQueryParameters();
					result = home.getQ4(queryMap.get("time").getFirst().trim().replaceAll(" ", "+"));
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
					
				case '5':
					queryMap = exchange.getQueryParameters();
					String place = queryMap.get("place").getFirst().trim();
					String startTime = queryMap.get("start_time").getFirst().trim().replaceAll(" ", "+");
					String endTime = queryMap.get("end_time").getFirst().trim().replaceAll(" ", "+");
					result = home.getQ5(place + "_" + startTime,
							place + "_" + endTime);
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
					
				case '6':
					queryMap = exchange.getQueryParameters();
					result = home.getQ6(queryMap.get("userid_min").getFirst().trim(),
							queryMap.get("userid_max").getFirst().trim());
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				}
			}
		}).build().start();
	}
}
