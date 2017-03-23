package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAAVReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		ArrayList<TFIDF_Tuple> idfs = new ArrayList<TFIDF_Tuple>();
		ArrayList<TFIDF_Tuple> tfs = new ArrayList<TFIDF_Tuple>();
		
		for (Text val: values) {
			String[] line_split = val.toString().split("\t");
			TFIDF_Tuple new_entry = new TFIDF_Tuple();
			new_entry.word = key.toString();
			if (line_split[0].equals("idf")){
				new_entry.idf = Float.parseFloat(line_split[1]);
				idfs.add(new_entry);
			}
			else if (line_split[0].equals("tf")) {
				new_entry.author = line_split[1];
				new_entry.tf_value = Float.parseFloat(line_split[2]);
				tfs.add(new_entry);
			}
			//ntext.write(new Text()), new Text("TYPE: " + line_split[0] + "ENTRY: " + new_entry.toString()));
		}
		
		//context.write(new Text("TFs: " + tfs.size()), new Text("IDFs: " + idfs.size()));
		for (TFIDF_Tuple idf: idfs) {
			for (TFIDF_Tuple tf: tfs) {
				if (idf.word.equals(tf.word)) {
					float tfidf = idf.idf * tf.tf_value;
					tf.tfidf_value = tfidf;
					context.write(new Text(tf.author), new Text(tf.word + "\t" + tf.tfidf_value));
				}
			}
		}
		/*for (Text val: values) {
			context.write(key, new Text(val));
		}*/
	}

}
