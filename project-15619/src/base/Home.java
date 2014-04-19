package base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

import javax.sql.DataSource;

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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Home {
	Connection driver;
	DataSource ds;
	HTable q2;
	HTable q3;
	HTable q4;
	HTable q5;
	HTable q6;

	public Home() {
		try {
			/*HikariConfig config = new HikariConfig();
			config.setMaximumPoolSize(80);
			config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
			config.addDataSourceProperty("url", "jdbc:mysql://localhost:3306/test");
			config.addDataSourceProperty("cachePrepStmts", true);
			config.addDataSourceProperty("useServerPrepStmts", true	);
			config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
			config.addDataSourceProperty("prepStmtCacheSize", 250);
			
			ds = new HikariDataSource(config);*/
			
			//HBase config
			q2 = new HTable(HBaseConfiguration.create(), Bytes.toBytes("q2"));
			q3 = new HTable(HBaseConfiguration.create(), Bytes.toBytes("q3"));
			q4 = new HTable(HBaseConfiguration.create(), Bytes.toBytes("q4"));
			q5 = new HTable(HBaseConfiguration.create(), Bytes.toBytes("q5"));
			q6 = new HTable(HBaseConfiguration.create(), Bytes.toBytes("q6"));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	/*
	public String getSQLEntries(String key) {
		try {
			Connection con = ds.getConnection();
			PreparedStatement stmt = con.prepareStatement("SELECT tweet_list FROM tweets_q2 WHERE user_time=?");
			stmt.setString(1, key);
			ResultSet set = stmt.executeQuery();
			StringBuffer results = new StringBuffer();

			while (set.next()) {
				results.append(set.getString("tweet_list").replaceAll("_", "\n"));
			}

			set.close();
			stmt.close();
			con.close();
			return results.toString();	
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getRetweets(String userid) {
		try {
			Connection connection = ds.getConnection();
			PreparedStatement stmt = connection.prepareStatement("SELECT retweet_user_list FROM tweets_q3 WHERE user_id=?");
			stmt.setString(1, userid);
			ResultSet rs = stmt.executeQuery();
			StringBuffer results = new StringBuffer();
			
			while(rs.next()) {
				results.append(rs.getString("retweet_user_list").replaceAll("_","\n"));
			}

			rs.close();
			stmt.close();
			connection.close();
			return results.toString();
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	} */
	
	//Q2
	public String getHBaseEntries(String key) {		
		Get g = new Get(Bytes.toBytes(key));
		Result r = null;
		try {
			r = q2.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] value = r.getValue(Bytes.toBytes("t"), Bytes.toBytes(""));
		String res = Bytes.toString(value);
		res = res.replaceAll("_", "\n");
		return res; 
	}

	//Q3
	public String getHBaseRetweets(String userid) {
		Get g = new Get(Bytes.toBytes(userid));
		Result r = null;
		try {
			r = q3.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] value = r.getValue(Bytes.toBytes("r"), Bytes.toBytes(""));
		String res = Bytes.toString(value);
		res = res.replaceAll("_", "\n");
		return res; 
	}
	
	/*
	//Q4
	public String getIdAndMessage(String time) {
		try {
			Connection connection = ds.getConnection();
			PreparedStatement stmt = connection.prepareStatement("SELECT msg FROM t WHERE time=?");
			stmt.setString(1, time);
			ResultSet rs = stmt.executeQuery();
			StringBuffer results = new StringBuffer();
			
			while(rs.next()) {
				String r = rs.getString("msg");
				results.append(r.substring(0, r.length()-1));
				results.append("\n");
			}

			rs.close();
			stmt.close();
			connection.close();
			return results.toString();
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}*/
	
	//Q6
	public String getTotalTweets(byte[] minId, byte[] maxId) {
		long sum = 0L;
		Scan scan = new Scan(minId, maxId);
		ResultScanner r = null;
		Result res = null;
		
		try {
			r = q6.getScanner(scan);
			while ((res = r.next()) != null) {
				sum += ByteBuffer.wrap(res.getValue(Bytes.toBytes("c"), Bytes.toBytes(""))).getLong();
			}
			r.close();
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
				Map<String, Deque<String>> queryMap = null;
				String userid = null;
				String result = null;
				
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");

				switch(path) {
				case '1':
					exchange.getResponseSender().send(ByteBuffer.wrap(info.concat(
							fmt.format(new Date())).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				case '2':
					queryMap = exchange.getQueryParameters();
					userid = queryMap.get("userid").getFirst().trim();
					String tweet_time = queryMap.get("tweet_time").getFirst().trim();
					result = home.getHBaseEntries(userid + "_" + tweet_time.replaceAll(" ", "+"));
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				case '3':
					queryMap = exchange.getQueryParameters();
					userid = queryMap.get("userid").getFirst().trim();
					result = home.getHBaseRetweets(userid);
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				case '4':
					queryMap = exchange.getQueryParameters();
					String time = queryMap.get("time").getFirst().trim();
					time = time.replace(" ", "+");
					//result = home.getIdAndMessage(time);
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				case '5':
					queryMap = exchange.getQueryParameters();
					String startTime = queryMap.get("start_time").getFirst().trim();
					String endTime = queryMap.get("end_time").getFirst().trim();
					String place = queryMap.get("place").getFirst().trim();
					break;
				case '6':
					queryMap = exchange.getQueryParameters();
					String minId = queryMap.get("userid_min").getFirst().trim();
					String maxId = queryMap.get("userid_max").getFirst().trim();
					result = home.getTotalTweets(minId.getBytes(), maxId.getBytes());
					exchange.getResponseSender().send(ByteBuffer.wrap(
							info.concat(result).getBytes(utf8)
							), IoCallback.END_EXCHANGE);
					break;
				}
			}
		}).build().start();
	}
}