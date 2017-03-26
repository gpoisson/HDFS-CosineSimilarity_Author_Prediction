package offline;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Count authors in a collection
public class AuthorCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		
		for (String line: lines) {
			String[] line_split = line.split("\t");
			String author = line_split[1];
			context.write( new Text("one"), new Text(author));
		}
	}
}
