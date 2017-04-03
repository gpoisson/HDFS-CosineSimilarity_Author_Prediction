// cs435_pa2 Assignment
// Author: Gregory Poisson
// Date: Mar 22, 2017
// Email: gpoisson@rams.colostate.edu

package offline;

// Data structure for storing multiple pieces of term-related information
public class TFIDF_Tuple {
	
	public double tf_value;
	public double idf;
	public double tfidf_value;
	public String word;
	public String author;
	
	public TFIDF_Tuple() {
		
	}
	
	public String toString() {
		return word + " " + author + ",  TF: " + tf_value + ",  IDF: " + idf + "  TFIDF: " + tfidf_value;
	}

}
