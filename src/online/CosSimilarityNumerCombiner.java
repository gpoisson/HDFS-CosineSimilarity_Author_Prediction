package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import offline.TFIDF_Tuple;

public class CosSimilarityNumerCombiner extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path path = new Path("mystery_data/adjusted_aav/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		
		int size = in.available();
		ArrayList<TFIDF_Tuple> mystery = new ArrayList<TFIDF_Tuple>();
		String line = new String();
		
		byte[] data = new byte[size];
		in.readFully(0, data);
		in.close();
		
		for (byte b: data){
			char next = (char) b;
			if (next == '\n'){
				TFIDF_Tuple entry = new TFIDF_Tuple();
				String[] split = line.split("\t");
				entry.word = split[1];
				entry.author = split[0];
				entry.tfidf_value = Float.parseFloat(split[2]);
				mystery.add(entry);
				line = new String();
			}
			else {
				line += next;
			}
		}
		
		float sum = (float) 0.0;
		for (Text val: values){
			String[] split = val.toString().split("\t");
			float tfidf = Float.parseFloat(split[0]);
			String term = split[1];
			
			for (TFIDF_Tuple entry: mystery){
				if (entry.word.equals(term)){
					sum += entry.tfidf_value * tfidf;
				}
			}
		}
		context.write(key, new Text(sum + ""));
	}
}
