package online;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterNewWordsMapper  extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		for (String line: lines) {
			
			String[] line_split = line.split("\t");
			
			if (line_split.length == 2){
				context.write(new Text(line_split[0]), new Text(line_split[1]));
			}
			if (line_split.length == 3){
				context.write(new Text(line_split[0] + "\t" + line_split[1]), new Text(line_split[2]));
			}
			
		}
	}
}
