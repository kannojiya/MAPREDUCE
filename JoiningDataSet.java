
import java.io.BufferedReader; 

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class JoiningDataSet {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();//hashmap for 1st file
		private Map<String, String> dsMap = new HashMap<String, String>();//hashmap for 2nd file
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		    Path p1 = new Path(files[1]);
		
		
			if (p.getName().equals("salary.txt")) 
			{
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) 
					{
						String[] tokens = line.split(",");
						String emp_id = tokens[0];
						String emp_sal = tokens[1];
						abMap.put(emp_id, emp_sal);
						line = reader.readLine();
					}
					reader.close();
			}
			if (abMap.isEmpty()) 
			{
				throw new IOException("Error:No File");
			}
			
			
			if (p1.getName().equals("desig.txt")) 
			{
					BufferedReader reader1 = new BufferedReader(new FileReader(p1.toString()));
					String line1 = reader1.readLine();
					while(line1 != null) 
					{
						String[] tokens1 = line1.split(",");
						String emp_id = tokens1[0];
						String emp_des = tokens1[1];
						dsMap.put(emp_id, emp_des);//getting empID and empName
						line1 = reader1.readLine();
					}
					reader1.close();
			}
			if (dsMap.isEmpty())
			{
				throw new IOException("Error:Unable to load the file");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	//entire data of the input or main file act as a key and data to be added from cache file will act as value 
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	String emp_id = tokens[0];
        	String salary = abMap.get(emp_id);//common column or data used for joining
        	String designation = dsMap.get(emp_id);//common column or data used for joining
        	String sal_des = salary + "," + designation;
        	outputKey.set(row);//entire data of input(main) file
        	outputValue.set(sal_des);//data in cache file(to be added or joined in the input file)
      	  	context.write(outputKey,outputValue);
        }  
}
	
	
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
  {
    Configuration conf = new Configuration();
	conf.set("mapred.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(JoiningDataSet.class);
    job.setJobName("Map Side Join");
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path("salary.txt").toUri());
    job.addCacheFile(new Path("desig.txt").toUri());
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
  }
}