package online;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CosSimilarityNumerSumReducer extends Reducer<Text,Text,Text,Text>{
	
	String[] author_names;
	double[] sums;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path path = new Path("data/author_names/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		
		int size = in.available();
		String author_line = new String();
		
		byte[] data = new byte[size];
		in.readFully(0, data);
		in.close();
		
		for (byte b: data){
			author_line += (char) b;
		}
		
		String[] author_names = author_line.split("\n");
		
		for (int i = 0; i < author_names.length; i++){
			author_names[i] = author_names[i].split("\t")[0];
		}
		this.author_names = author_names;
		this.sums = new double[author_names.length];
	}
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		for (Text val: values){
			String author = key.toString();
			String[] split = val.toString().split("\t");
			String term = split[0];
			double product = Double.parseDouble(split[1]);
			for (int i = 0; i < author_names.length; i++){
				if (author.equals(author_names[i])){
					sums[i] += product;
					break;
				}
			}
			
		}
		for (int i = 0; i < author_names.length; i++){
			if (author_names[i].equals(key.toString())){
				context.write(new Text(key.toString()), new Text(sums[i] + ""));
			}
		}
		//for (int i = 0; i < author_names.length; i++){
		//	context.write(new Text(author_names[i]), new Text(sums[i] + ""));
		//}
	}
}
