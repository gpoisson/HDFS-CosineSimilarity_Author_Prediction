package offline;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EqualizeAAVMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		//input is <term> <author/idf		tf/"idf">
		for (String line: lines) {
			String term = key.toString();
			context.write(new Text(term), new Text(line));
		}
	}
}
