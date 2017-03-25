package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterNewWordsMapper  extends Mapper<Text, Text, Text, Text>{
	
	public void map(Text   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		for (String line: lines) {
			String[] line_split = line.split("\t");
			
			String author = line_split[0];
			if (author.substring(0, 4).equals("idf_")){
				context.write(new Text(), new Text());
			}
			else {
				context.write(new Text(), new Text());
			}
		}
	}
}
