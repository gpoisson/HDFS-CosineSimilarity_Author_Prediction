package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Count the number of authors who use a specific word
public class AuthorWordUseCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		ArrayList<String> terms = new ArrayList<String>();
		
		for (String line: lines) {
			String[] line_split = line.split("\t");
			String term = line_split[0];
			terms.add(term);
		}
		for (String term: terms) {
			context.write(new Text(term), new Text("one"));
		}
	}
}
