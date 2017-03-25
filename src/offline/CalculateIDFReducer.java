package offline;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateIDFReducer extends Reducer<Text,Text,Text,FloatWritable> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path path = new Path("data/author_count/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		
		int size = in.available();
		String author_line = new String();
		
		byte[] data = new byte[size];
		in.readFully(0, data);
		in.close();
		
		for (byte b: data){
			author_line += (char) b;
		}
		
		String[] line_split = author_line.split("\t");
		
		float author_count = Float.parseFloat(line_split[1]);
		
		for (Text val: values) {
			float idf = (float) Math.log(author_count / Float.parseFloat(val.toString()));
			context.write(new Text("idf_" + key.toString()), new FloatWritable(idf));
		}
	}
}
