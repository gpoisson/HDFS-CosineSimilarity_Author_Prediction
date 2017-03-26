package online;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Calculate cosine similarity
public class CosSimilarityDenomCombiner  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		float sq_sum = (float) 0.0;
		for (Text val: values){
			String[] split = val.toString().split("\t");
			String tfidf = split[0];
			float tfidf_value = Float.parseFloat(tfidf);
			sq_sum += (tfidf_value * tfidf_value);
		}
		float root_sq_sum = (float) Math.sqrt(sq_sum);
		context.write(key, new Text(root_sq_sum + ""));
	}

}
