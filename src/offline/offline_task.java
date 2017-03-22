package offline;

import java.io.IOException;
import java.util.ArrayList;

public class offline_task {
	
	public static void main(String[] args) throws IOException {
		// 1.	Build global word counts for all collections
		System.out.println("Building global word count...");
		GlobalCount gc = new GlobalCount();
		gc.build_global_word_count();
		
		/* 2.	For each collection
		 * 			Determine most frequently used word
		 * 			Construct TF array
		 * 				TF = 0.5 + 0.5(fij / maxk fkj)
		 */
		System.out.println("Building TF arrays...");
		ArrayList<WordTuple> most_common_words = new ArrayList<WordTuple>();
		ArrayList<TFArray> tf_arrays = new ArrayList<TFArray>();
		for (int author_index = 0; author_index < gc.authors.size(); author_index++) {
			WordTuple most_common = gc.most_common_word(author_index);
			most_common_words.add(most_common);
			TFArray tf_array = new TFArray(most_common, gc.authors.get(author_index));
			tf_array.compute();
			tf_arrays.add(tf_array);
		}
		
		
		/*
		 * 3.	Use global word count to construct IDF arrays
		 * 			IDFi = log (N / ni)
		 * 			N: Number of authors
		 * 			ni: Number of authors who use the term <i>
		 */
		System.out.println("Building IDF arrays...");
		int N = gc.authors.size();
		ArrayList<IDFArray> idf_arrays = new ArrayList<IDFArray>();
		for (int author_index = 0; author_index < gc.authors.size(); author_index++) {
			IDFArray idf = new IDFArray(N, gc.authors.get(author_index), gc);
			idf.compute();
			idf_arrays.add(idf);
			System.out.println("   " + idf_arrays.size() + "/" + N + " IDF arrays computed...");
		}
		
		System.out.println(tf_arrays.size() + " TF arrays computed.");
		System.out.println(tf_arrays.get(0).tf_array.get(0));
		System.out.println(idf_arrays.size() + " IDF arrays computed");
		System.out.println(idf_arrays.get(0).idf_array.get(0));
		
		
		/*
		 * 4.	For each author:
		 * 			Construct TF-IDF array:
		 * 				TF-IDF = TFij * IDFi
		 */
		
		
		/*
		 * 5.	Make all TF-IDF vectors the same dimension
		 * 			For each author:
		 * 				Add an element to the TF-IDF vector = 0 for each
		 * 				term used by another author which ithis author
		 * 				does not use (use global word count)
		 */
		
	}

}
