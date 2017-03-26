package offline;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Compute Inverted Document Frequency
public class CalculateIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");		
		
		for (String line: lines) {
			String[] line_split = line.split("\t");
			assert (line_split.length == 2);
			String word = line_split[0];
			int count = Integer.parseInt(line_split[1]);
			context.write(new Text(word), new Text(count + ""));
		}		
	}
}
