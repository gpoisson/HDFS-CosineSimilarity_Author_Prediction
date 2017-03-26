package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Compute Author Attribute Vector
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
		}
		
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
		
		for (TFIDF_Tuple idf: idfs) {
			for (String author: author_names) {
				String name = author.split("\t")[0];
				boolean found = false;
				for (TFIDF_Tuple tf: tfs) {
					if (tf.author.equals(name) && tf.word.equals(idf.word)){
						found = true;
						break;
					}
				}
				if (!found) {
					TFIDF_Tuple new_entry = new TFIDF_Tuple();
					new_entry.word = idf.word;
					new_entry.author = name;
					new_entry.tf_value = (float) (0.5);
					tfs.add(new_entry);					
				}
			}
		}
		
		for (TFIDF_Tuple idf: idfs) {
			for (TFIDF_Tuple tf: tfs) {
				if (idf.word.equals(tf.word)) {
					float tfidf = idf.idf * tf.tf_value;
					tf.tfidf_value = tfidf;
					context.write(new Text(tf.author), new Text(tf.word + "\t" + tf.tfidf_value));
				}
			}
		}
	}

}
