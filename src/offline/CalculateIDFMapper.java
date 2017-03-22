package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		int author_count = 0;
		ArrayList<String> terms = new ArrayList<String>();
		ArrayList<Integer> counts = new ArrayList<Integer>();
		
		
		for (String line: lines) {
			String[] line_split = line.split("\t");
			if (line_split[0].equals("author:")) {
				author_count++;
			}
			else {
				terms.add(line_split[0]);
				counts.add(Integer.parseInt(line_split[2]));
			}
		}
		
		for (int i = 0; i < terms.size(); i++) {
			context.write(new Text(terms.get(i)), new Text(counts.get(i) + ""));
		}
		context.write(new Text("authors:"), new Text(author_count + ""));
		
		
	}
}
