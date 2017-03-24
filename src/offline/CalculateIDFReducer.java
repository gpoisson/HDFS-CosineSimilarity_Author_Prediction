package offline;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateIDFReducer extends Reducer<Text,Text,Text,FloatWritable> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path path = new Path("data/author_count/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		
		int size = in.available();
		String author_line = new String();
		
		byte[] data = new byte[size];
		in.readFully(0, data);
		in.close();
		
		for (byte b: data){
			author_line += (char) b;
		}
		
		String[] line_split = author_line.split("\t");
		
		float author_count = Float.parseFloat(line_split[1]);
		
		for (Text val: values) {
			float idf = (float) Math.log(author_count / Float.parseFloat(val.toString()));
			context.write(new Text("idf_" + key.toString()), new FloatWritable(idf));
		}
	}
}
