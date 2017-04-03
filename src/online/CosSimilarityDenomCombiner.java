package online;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Calculate cosine similarity
public class CosSimilarityDenomCombiner  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		double sq_sum = 0.0;
		for (Text val: values){
			String[] split = val.toString().split("\t");
			String tfidf = split[0];
			double tfidf_value = Double.parseDouble(tfidf);
			sq_sum += (tfidf_value * tfidf_value);
		}
		double root_sq_sum = Math.sqrt(sq_sum);
		context.write(key, new Text(root_sq_sum + ""));
	}

}
