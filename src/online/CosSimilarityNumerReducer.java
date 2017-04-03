package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import offline.TFIDF_Tuple;

public class CosSimilarityNumerReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		ArrayList<TFIDF_Tuple> knowns = new ArrayList<TFIDF_Tuple>();
		ArrayList<TFIDF_Tuple> unknowns = new ArrayList<TFIDF_Tuple>();
		
		for (Text val: values){
			String term = key.toString();
			String[] split = val.toString().split("\t");
			String author = split[0];
			float tfidf = Float.parseFloat(split[1]);
			TFIDF_Tuple entry = new TFIDF_Tuple();
			entry.word = term;
			entry.author = author;
			entry.tfidf_value = tfidf;
			if (entry.author.equals("xyz")){
				unknowns.add(entry);
			}
			else {
				knowns.add(entry);
			}
		}
		
		for (TFIDF_Tuple known: knowns){
			for (TFIDF_Tuple unk: unknowns){
				float product = known.tfidf_value * unk.tfidf_value;
				context.write(new Text(known.author), new Text(known.word + "\t" + product));
			}
		}
	}
}
