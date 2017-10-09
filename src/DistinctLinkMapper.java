import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DistinctLinkMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{
//		System.out.println("In the mapper");
		String line = value.toString();
		String[] data = line.split(" ");
		String webSite = data[0];
		String link = data[1];
		output.collect(new Text(webSite), new Text(link));
	}
}