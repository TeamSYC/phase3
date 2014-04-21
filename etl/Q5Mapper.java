import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.*;

public class Q5Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Gson gson = new GsonBuilder().create();
	private static final SimpleDateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZ yyyy");
	private static final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
	private Collection<String> places;

	@Override
	public void setup(Context context) {
		places = context.getConfiguration().getStringCollection("places");
	}

	private String sanitize(String input) {
	    String[] str = input.replaceAll("[^a-zA-Z]", " ").split("\\s+");
	    String result = " ";
	    for (String token : str) {
		  result += token.trim() + " ";
	    }
	    return result;
	}

	@Override
	protected void map(LongWritable lineNumber, Text line, Context context) 
			throws IOException, InterruptedException {

		Object o = gson.fromJson(line.toString(), Object.class);
		Map<String, String> map = (Map<String, String>)o;
		String text = map.get("text");
		String tweetId = map.get("id_str");
		String timestamp = null;

		text = sanitize(text);
		try {
			timestamp = outputFormat.format(inputFormat.parse(map.get("created_at")));
		} catch(ParseException e) {
			return;
		}

		for (String p : places) {
			if (text.matches(".*\\s" + p + "\\s.*")) {
				String key = p + "_" + timestamp;
				context.write(new Text(key), new Text(tweetId));
			}
		}
	}
}