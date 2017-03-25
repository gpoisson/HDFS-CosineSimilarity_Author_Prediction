package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import offline.TFIDF_Tuple;

public class CosSimilarityMapper  extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		ArrayList<TFIDF_Tuple> knowns = new ArrayList<TFIDF_Tuple>();
		ArrayList<TFIDF_Tuple> mystery = new ArrayList<TFIDF_Tuple>();
		for (String line: lines) {
			String[] line_split = line.split("\t");
			
		}
	}
}
