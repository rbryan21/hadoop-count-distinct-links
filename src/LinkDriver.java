import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class LinkDriver
{
	public static void main(String[] args)
	{
		JobClient myClient = new JobClient();
		//Crete a configuration object for the job
		JobConf jobConf = new JobConf(LinkDriver.class);
		
		//Set a name of the Job
		jobConf.setJobName("CountDistinctLinks");
		
		//Specify data type of output key and value
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		//Specify names of Mapper and Reducer class
		jobConf.setMapperClass(DistinctLinkMapper.class);
		jobConf.setReducerClass(DistinctLinkReducer.class);
		
		//Specify formats of the data type of Input and output
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		
		//Set input and output directories using command line arguments
		//arg[0] = name of input directory on HDFS and arg[1] = name of output directory to be created
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		myClient.setConf(jobConf);
		try
		{
			//run the job
			JobClient.runJob(jobConf);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
