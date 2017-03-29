package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateAAVReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		// Load author names list
		// Determine whether an author used a word
		
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
		for (String name: author_names){
			name = name.split("\t")[0];
		}
		
		ArrayList<TFIDF_Tuple> tfs = new ArrayList<TFIDF_Tuple>();
		ArrayList<TFIDF_Tuple> idfs = new ArrayList<TFIDF_Tuple>();
		
		for (Text val: values) {
			String[] split = val.toString().split("\t");
			// IDF
			if (split.length == 1) {
				TFIDF_Tuple idf = new TFIDF_Tuple();
				idf.word = key.toString();
				idf.idf = Float.parseFloat(split[0]);
				idfs.add(idf);
			}
			// TF
			else if (split.length == 2) {
				TFIDF_Tuple tf = new TFIDF_Tuple();
				tf.word = key.toString();
				tf.author = split[1];
				tf.tf_value = Float.parseFloat(split[1]);
				tfs.add(tf);
			}
		}
		
		for (TFIDF_Tuple idf: idfs){
			boolean found[] = new boolean[author_names.length];
			for (boolean f: found){
				f = false;
			}
			for (TFIDF_Tuple tf: tfs) {
				if (idf.word.equals(tf.word)){
					float tfidf = tf.tf_value * idf.idf;
					tf.tfidf_value = tfidf;
					for (int i = 0; i < author_names.length; i++){
						if (author_names[i].equals(tf.author)){
							found[i] = true;
						}
					}
					context.write(new Text(tf.author), new Text(tf.word + "\t" + tf.tfidf_value));
				}
			}
			for (int i = 0; i < author_names.length; i++){
				if (found[i] == false){
					float tfidf = (float) Math.log(author_names.length);
					context.write(new Text(author_names[i]), new Text(idf.word + "\t" + tfidf));
				}
			}
		}
		
	}
}
