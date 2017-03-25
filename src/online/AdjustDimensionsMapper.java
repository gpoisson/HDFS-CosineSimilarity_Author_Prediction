package online;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AdjustDimensionsMapper  extends Mapper<Text, Text, Text, Text>{
	
	public void map(Text   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		for (String line: lines) {
			
			String[] line_split = line.split("\t");
			
			context.write(new Text(line_split[0]), new Text(line_split[1]));
			
		}
	}
}
