import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class STDCallDuration 
{
	public static class MapperClass extends Mapper <Object, Text, Text, IntWritable>
	{
		Text PhoneNum = new Text();
		IntWritable durations = new IntWritable();
		
		public void map (Object key, Text value, Context con) throws IOException, InterruptedException
		{
		try
			{
				String [] str = value.toString().split(",");
				if (str[4].equals("1"))
				{
				PhoneNum.set(str[0]);
				String call_end_time = str[3];
				String call_start_time = str[2];
				long dur = toMilliSec(call_end_time)-toMilliSec(call_start_time);
				durations.set((int)(dur/(1000*60)));
				con.write(PhoneNum, durations);
				}
			}
			catch(Exception e)
			  {
				  e.printStackTrace();
			  }
		}
		private long toMilliSec(String date)
		{
		    
		     SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		     Date dateFrm = null;
		     try 
		     {
		         dateFrm = format.parse(date);
		     }
		     catch (Exception e)
		     {

		         e.printStackTrace();
		     }
		     return dateFrm.getTime();
		 }
	}
	
	public static class IntSumReducer extends Reducer <Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce (Text key, Iterable<IntWritable> value, Context con) throws IOException, InterruptedException
		{
			long sum = 0;
			for (IntWritable val:value)
			{
				sum += val.get();
			}
			result.set((int)sum);
			if (sum>=60)
			{
				con.write(key, result);
			}
		}
	}
	 public static void main(String args[]) throws Exception
	    {
	        Configuration conf = new Configuration();
	        conf.set("mapred.textoutputformat.separator",",");//denoting the output by what separator should be used
	        Job job = Job.getInstance(conf, "STDCallDuration");
	        job.setJarByClass(STDCallDuration.class);
	        job.setMapperClass(MapperClass.class);
	        job.setCombinerClass(IntSumReducer.class);
	        job.setReducerClass(IntSumReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }   
}
