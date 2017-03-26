package online;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosSimilarityNumerCombiner extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		float sum = (float) 0.0;
		for (Text val: values){
			String[] split = val.toString().split("\t");
			String tfidf = split[0];
			float tfidf_value = Float.parseFloat(tfidf);
			sum += tfidf_value;
		}
		context.write(key, new Text(sum + ""));
	}
}
