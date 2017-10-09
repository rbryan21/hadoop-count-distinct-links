import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DistinctLinkReducer extends MapReduceBase implements 
	Reducer<Text, Text, Text, Text>
{

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{
		Set<String> links = new HashSet<>();
		while (values.hasNext())
		{
			Text value = (Text) values.next();
			links.add(value.toString());
		}
		output.collect(key, new Text(links.size() + ""));
	}

}
