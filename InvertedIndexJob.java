import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndexJob {

public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

    
    private Text word = new Text();
    Text docID = new Text();
	
    public void map(LongWritable key, Text value,Context context) throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
	  String docid = itr.nextToken();
	  docID.set(docid);
	  while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, docID);
      }
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable < Text > values,
                Context context) throws IOException, InterruptedException  {
				
		HashMap < String, Integer > hash_map = new HashMap < String, Integer > ();		
		Iterator < Text > itr1 = values.iterator();		
				
      while (itr1.hasNext()) {
        String v = itr1.next().toString();
		if(!hash_map.containsKey(v))
		   hash_map.put(v,1);
		   else
		   {
		   int count = hash_map.get(v);
		   count++;
		   hash_map.put(v,count);
      }
      
    }
	StringBuffer sb = new StringBuffer("");
	for (Map.Entry < String, Integer > map: hash_map.entrySet())
	{
	sb.append(map.getKey() + ":" + map.getValue() + "\t");
  }
  Text val =new Text(sb.toString());
  context.write(key,val);
  }
  }
  
  public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "inverted index");
  job.setJarByClass(InvertedIndexJob.class);
  job.setJobName("Inverted Index");
  
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
  job.setMapperClass(MapClass.class);
  job.setReducerClass(IntSumReducer.class);
  
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  
  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  }