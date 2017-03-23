package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateAAVMapper extends Mapper<LongWritable, Text, Text, Text> {

public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		
	String[] lines = value.toString().split("\n");
	ArrayList<TFIDF_Tuple> idfs = new ArrayList<TFIDF_Tuple>();
	ArrayList<TFIDF_Tuple> tfs = new ArrayList<TFIDF_Tuple>();
	
	for (String line: lines) {
		String[] line_split = line.split("\t");
		String author = new String();
		String word = new String();
		if (line_split[0].substring(0, 4).equals("idf_")){
			word = line_split[0].substring(4);
			float idf = Float.parseFloat(line_split[1]);
			boolean found = false;
			TFIDF_Tuple new_entry = new TFIDF_Tuple();
			new_entry.word = word;
			new_entry.idf = idf;
			idfs.add(new_entry);
			context.write(new Text(word), new Text("idf\t" + idf ));
		}
		else if (line_split[0].substring(0, 3).equals("tf_")){
			author = line_split[0].substring(3);
			word = line_split[1];
			float tf = Float.parseFloat(line_split[2]);
			TFIDF_Tuple new_entry = new TFIDF_Tuple();
			new_entry.word = word;
			new_entry.author = author;
			new_entry.tf_value = tf;
			tfs.add(new_entry);
			context.write(new Text(word), new Text("tf\t" + new_entry.author + "\t" + new_entry.tf_value));
			break;
			
			/*
			if (!found) {
				TFIDF_Tuple new_entry = new TFIDF_Tuple();
				new_entry.word = word;
				new_entry.tf_values.add(tf);
				new_entry.authors.add(author);
				tfidfs.add(new_entry);
			}*/
			//context.write(new Text(word), new Text("tf\t" + author + "\t" + tf));
		}
	}
	
	/*context.write(new Text("TF array size: " + tfs.size()), new Text("IDF array size: " + idfs.size()));
	
	for (TFIDF_Tuple idf: idfs) {
		for (TFIDF_Tuple tf: tfs) {
			if (idf.word.equals(tf.word)) {
				float tfidf = idf.idf * tf.tf_value;
				tf.tfidf_value = tfidf;
				context.write(new Text(tf.author), new Text(tf.word + "\t" + tf.tfidf_value));
			}
		}
	}*/
	
	/*
	for (TFIDF_Tuple entry: tfidfs) {
		for (int i = 0; i < entry.tf_values.size(); i++){
			//float tfidf = entry.tf_values.get(i) * entry.idf.get(0);
			//entry.tfidf_values.add(tfidf);
			//context.write(new Text(entry.authors.get(i)), new Text(entry.word + " " + entry.tf_values.get(i) + " " + entry.idf + "   " + tfidf));
		}
	}*/
}

}
