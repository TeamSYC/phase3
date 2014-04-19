import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;  
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;

public class Q5Driver {

   public String getPlaces(String places) throws Exception {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(places)));
      String line = null;
      String result = "";

      while ((line = reader.readLine()) != null) {
          result += line.trim() + ",";
      }

      return result.substring(0, result.length()-1);
   }

   public static void main(String[] args) throws Exception {
      Configuration config = new Configuration();
      config.set("places", new Q5Driver().getPlaces("/home/hadoop/Q5/places.txt"));
      Job job = new Job(config, "Q5");
      String input = "Q5";
      String output = "Output";

      //Job settings
      job.setJarByClass(Q5Driver.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(Q5Mapper.class);
      job.setReducerClass(Q5Reducer.class);

      FileInputFormat.setInputPaths(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path(output));

      job.waitForCompletion(true);
   } 
}
