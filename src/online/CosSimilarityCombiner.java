package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import offline.TFIDF_Tuple;

// Calculate cosine similarity
public class CosSimilarityCombiner  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		float sum = (float) 0.0;
		float prod_sum = (float) 0.0;
		for (Text val: values){
			String author = key.toString();
			String[] split = val.toString().split("\t");
			String tfidf = split[0];
			String term = split[1];
			
			float tfidf_value = Float.parseFloat(tfidf);
			sum += tfidf_value;
		}
		context.write(key, new Text(sum + ""));
	}

}
