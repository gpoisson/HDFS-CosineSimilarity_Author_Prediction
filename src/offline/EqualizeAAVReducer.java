package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EqualizeAAVReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		/*
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
		boolean[] found = new boolean[author_names.length];
		
		for (int a = 0; a < author_names.length; a++){
			author_names[a] = author_names[a].split("\t")[0];
			found[a] = false;
		}
		*/
		ArrayList<TFIDF_Tuple> idfs = new ArrayList<TFIDF_Tuple>();
		ArrayList<TFIDF_Tuple> tfs = new ArrayList<TFIDF_Tuple>();
		
		//input is <term> <author/idf		tf/"idf">
		for (Text val: values){
			String term = key.toString();
			String[] split = val.toString().split("\t");
			// IDF
			if (split[2].equals("idf")){
				TFIDF_Tuple entry = new TFIDF_Tuple();
				entry.word = term;
				entry.idf = Float.parseFloat(split[1]);
				idfs.add(entry);
			}
			// TF
			else {
				TFIDF_Tuple entry = new TFIDF_Tuple();
				entry.word = term;
				entry.tf_value = Float.parseFloat(split[2]);
				tfs.add(entry);
			}
		}
		
		for (TFIDF_Tuple idf: idfs){
			for (TFIDF_Tuple tf: tfs){
				float tfidf = tf.tf_value * idf.idf;
				context.write(new Text(tf.author), new Text(tf.word + "\t" + tfidf));
			}
		}
		
	}
}
