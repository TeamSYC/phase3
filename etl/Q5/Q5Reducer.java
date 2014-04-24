import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5Reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String val = "";
		for (Text v : values) {
			val += v.toString() + "_";
		}
		context.write(key, new Text(val));
	}
}
