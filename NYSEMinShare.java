import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NYSEMinShare //to find maximum of each share symbol.
{

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable>
  {

	  public void map(LongWritable key, Text value, Context context)
	  {
		  try{
			  String[] str = value.toString().split(",");
			  long vol = Long.parseLong(str[7]);
			  context.write(new Text(str[1]), new LongWritable(vol));
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
		  }
	  }
  }

  public static class IntSumReducer extends Reducer<Text,LongWritable,Text,LongWritable> 
  {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
      long min = 999999999;
      for (LongWritable val:values)
      if (min > val.get())
      {
    	  min = val.get();
      }
      result.set(min);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(NYSEMinShare.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

