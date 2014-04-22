package base;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.io.IoCallback;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class Home {
	private final static int NUM_CONNECTIONS = 100;
	private static HTablePool pool;
	private static HTableInterface q2;
	private static HTableInterface q3;
	private static HTableInterface q4;
	private static HTableInterface q5;
	private static HTableInterface q6;
	private final byte[] T = Bytes.toBytes("t");
	private final byte[] R = Bytes.toBytes("r");
	private final byte[] C = Bytes.toBytes("c");
	private final byte[] EMPTY = Bytes.toBytes("");
	private ConcurrentMap<String, String> cache3;

	public Home() throws Exception {
		cache3 = new ConcurrentLinkedHashMap.Builder<String, String>()
				.maximumWeightedCapacity(3000000)
				.build();
		warmUpCache();
		pool = new HTablePool(HBaseConfiguration.create(), NUM_CONNECTIONS);
		q2 = pool.getTable(Bytes.toBytes("q2"));
		q3 = pool.getTable(Bytes.toBytes("q3"));
		q4 = pool.getTable(Bytes.toBytes("q4"));
		q5 = pool.getTable(Bytes.toBytes("q5"));
		q6 = pool.getTable(Bytes.toBytes("q6"));
	}

	public void warmUpCache() {
		System.out.println("warm up get started!!!");
		long linenum = 0;
		for(int i = 0 ; i <= 55; i++) {
			String filename = "part-000" + String.format("%02d",i);
			try {
				BufferedReader br = new BufferedReader(new FileReader("/home/ubuntu/q3/" + filename));
				String line;
				while ((line = br.readLine()) != null) {
					String[] part = line.split(",");
					cache3.put(part[0], part[1].trim().replaceAll("_", "\n"));
					linenum ++;
				}
				br.close();
				System.out.println(filename + " is done!!!!!!");
			} catch(Exception e) {
				e.printStackTrace();
				return;
			}
		}

		System.out.println("q3 warm up get finished!!!");
		System.out.println("line_num is " + linenum);
	}

	public String getQ2(String key) throws Exception {
		Get g = new Get(Bytes.toBytes(key));
		return Bytes.toString(q2.get(g).getValue(T, EMPTY)); 
	}

	public String getQ3(String userid) throws Exception {
		if (cache3.containsKey(userid)) {
			return cache3.get(userid);
		}
		Get g = new Get(Bytes.toBytes(userid));
		Result r =  q3.get(g);
		String result = Bytes.toString(r.getValue(R, EMPTY));
		cache3.put(userid, result);
		return result;
	}

	public String getQ4(String time) throws Exception {
		Get g = new Get(Bytes.toBytes(time));
		return Bytes.toString(q4.get(g).getValue(T, EMPTY));
	}

	public String getQ5(String start, String end) throws Exception {
		byte[] origEnd = Bytes.toBytes(end);
		Scan scan = new Scan(Bytes.toBytes(start), Arrays.copyOf(origEnd, origEnd.length+1));
		ResultScanner r = null;
		Result res = null;
		String result = "";
		r = q5.getScanner(scan);
		while ((res = r.next()) != null) {
			result += Bytes.toString(res.getValue(T, EMPTY));
		}
		return result;
	}

	public String getQ6(String min, String max) throws Exception {
		if (max.compareTo(min) < 0 || min.compareTo("0000000016") < 0 
				|| max.compareTo("2427052445") > 0) {
			return null;
		}
		Scan scan1 = new Scan(Bytes.toBytes(min));
		Scan scan2 = new Scan(Bytes.toBytes(max));
		scan1.setBatch(1);
		scan2.setBatch(1);	
		scan1.setCacheBlocks(true);
		scan2.setCacheBlocks(true);
		ResultScanner minScanner = null;
		ResultScanner maxScanner = null;
		long result = 0L;
		minScanner = q6.getScanner(scan1);
		maxScanner = q6.getScanner(scan2);
		Result minRow = minScanner.next();
		Result maxRow = maxScanner.next();
		result = Long.parseLong(Bytes.toString(maxRow.getValue(C, EMPTY)))
				- Long.parseLong(Bytes.toString(minRow.getValue(C, EMPTY)));
		minScanner.close();
		maxScanner.close();
		return result+"\n";
	}

	public static void main(String[] args) throws Exception {
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
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");

				switch (path) {
				case '1':
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(fmt.format(new Date())).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;

				case '2':
					queryMap = exchange.getQueryParameters();
					result = home.getQ2(queryMap.get("userid").getFirst() + "_" +
							queryMap.get("tweet_time").getFirst().replaceAll(" ", "+"));
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;

				case '3':
					queryMap = exchange.getQueryParameters();
					result = home.getQ3(queryMap.get("userid").getFirst());
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;

				case '4':
					queryMap = exchange.getQueryParameters();
					result = home.getQ4(queryMap.get("time").getFirst().replaceAll(" ", "+"));
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;

				case '5':
					queryMap = exchange.getQueryParameters();
					String place = queryMap.get("place").getFirst();
					String startTime = queryMap.get("start_time").getFirst().replaceAll(" ", "+");
					String endTime = queryMap.get("end_time").getFirst().replaceAll(" ", "+");
					result = home.getQ5(place + "_" + startTime,
							place + "_" + endTime);
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;

				case '6':
					queryMap = exchange.getQueryParameters();
					result = home.getQ6(String.format("%010d", Long.parseLong(
							queryMap.get("userid_min").getFirst())), 
							String.format("%010d", Long.parseLong(queryMap.get("userid_max").getFirst())+1));
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				}
			}
		}).build().start();
	}
}