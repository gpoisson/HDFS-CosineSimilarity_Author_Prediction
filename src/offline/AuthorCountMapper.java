package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AuthorCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		ArrayList<String> authors = new ArrayList<String>();
		for (String line: lines) {
			String[] split_line = line.split("\t");
			String author = split_line[1];
			boolean found = false;
			for (String known: authors) {
				if (known.equals(author)) {
					found = true;
					break;
				}
			}
			if (!found) {
				authors.add(author);
			}
		}		
		for (String author: authors) {
			context.write(new Text("authors:"), new Text("one"));
		}
	}
}
