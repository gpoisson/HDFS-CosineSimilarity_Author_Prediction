package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateTFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		ArrayList<String> keys = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		for (String line: lines) {
			line = line.toLowerCase();
		
			String[] line_split = line.split("\t");
			String word = line_split[0];
			String author = line_split[1];
			int count = Integer.parseInt(line_split[2]);
			
			keys.add(author);
			values.add(word + "\t" + count);
		}

		for (int i = 0; i < keys.size(); i++) {
			context.write(new Text(keys.get(i)), new Text(values.get(i)));
		}
	}
}
