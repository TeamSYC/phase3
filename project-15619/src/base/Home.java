package base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.hadoop.hbase.util.Bytes;

public class Home {
	Connection driver;
	//DataSource ds;
	HTable table;

	public Home() {
		try {
			/*
			HikariConfig config = new HikariConfig();
			config.setMaximumPoolSize(80);
			config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
			config.addDataSourceProperty("url", "jdbc:mysql://localhost:3306/tweet_db");
			config.addDataSourceProperty("user", "root");
			config.addDataSourceProperty("password", "db15319root");
			config.addDataSourceProperty("cachePrepStmts", true);
			config.addDataSourceProperty("useServerPrepStmts", true	);
			config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
			config.addDataSourceProperty("prepStmtCacheSize", 250);
			
			ds = new HikariDataSource(config);
			*/
			//HBase config
			table = new HTable(HBaseConfiguration.create(), Bytes.toBytes("tweets"));
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
	
	public String getHBaseEntries(String key) {		
		Get g = new Get(Bytes.toBytes(key));
		Result r = null;
		try {
			r = table.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//		try {
		//			table.close();
		//		} catch (IOException e) {
		//			e.printStackTrace();
		//		}
		byte[] value = r.getValue(Bytes.toBytes("tl"), Bytes.toBytes(""));
		String res = Bytes.toString(value);

		res = res.replaceAll("_", "\n");
		return res; 
	}

	public String getHBaseRetweets(String userid) {
		Get g = new Get(Bytes.toBytes(userid));
		Result r = null;
		try {
			r = table.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//		try {
		//			table.close();
		//		} catch (IOException e) {
		//			e.printStackTrace();
		//		}
		byte[] value = r.getValue(Bytes.toBytes("rt"), Bytes.toBytes(""));
		String res = Bytes.toString(value);

		res = res.replaceAll("_", "\n");
		return res; 
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
				
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");

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
					userid = queryMap.get("time").getFirst().trim();
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
					break;
				}
			}
		}).build().start();
	}
}