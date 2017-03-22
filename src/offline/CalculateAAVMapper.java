package offline;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateAAVMapper extends Mapper<LongWritable, Text, Text, Text> {

public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
	String[] lines = value.toString().split("\n");
	for (String line: lines) {
		String[] line_split = line.split("\t");
		String word = new String();
		if (line_split[0].substring(0, 4).equals("idf_")){
			word = line_split[0].substring(5);
			float idf = Float.parseFloat(line_split[1]);
			context.write(new Text(word + " idf"), new Text(idf + ""));
		}
		else if (line_split[1].substring(0, 3).equals("tf_")){
			word = line_split[1].substring(4);
			float tf = Float.parseFloat(line_split[2]);
			context.write(new Text(line_split[0] + " " + word + " tf"), new Text(tf + ""));
		}
	}
		context.write(new Text("test"), new Text("one"));
	}
}
