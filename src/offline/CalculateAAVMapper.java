package offline;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateAAVMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
			
		String[] lines = value.toString().split("\n");
		
		for (String line: lines) {
			String[] split = line.split("\t");
			String term = split[0];
			if (term.substring(0, 3).equals("tf_")){
				String author = split[1];
				String tf = split[2];
				context.write(new Text(term.substring(3)), new Text(author + "\t" + tf));
			}
			else if (term.substring(0, 4).equals("idf_")){
				String idf = split[1];
				context.write(new Text(term.substring(4)), new Text(idf + ""));
			}
		}
	}

}
