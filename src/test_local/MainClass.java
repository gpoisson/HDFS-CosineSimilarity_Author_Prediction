package test_local;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MainClass {
	
	public static void main(String[] args) throws IOException {
	
		String textFile = "testData.txt";
		BufferedReader reader;
		
		reader = new BufferedReader(new FileReader(textFile));
		String line;
		
		ArrayList<String> authors = new ArrayList<String>();
		ArrayList<ArrayList<String>> text = new ArrayList<ArrayList<String>>();
		
		while ((line = reader.readLine()) != null) {
			String[] line_split = line.split("<===>");
			String author = line_split[0];
			//String date = line_split[1];
			String book_text = line_split[2];
			int auth_index = increment_author(author, authors);
			add_text(auth_index, book_text, text);
		}
		for (int i = 0; i < authors.size(); i++){
			System.out.println(authors.get(i) + " wrote " + text.get(i).size() + " total words -- (" + count_unique_words(i, text) + " unique words).");
			System.out.println("  Most common word: " + most_common(text.get(i)));
			System.out.println("  Least common word: " + least_common(text.get(i)));
		}
		reader.close();
	}
	
	public static String least_common(ArrayList<String> text) {
		String least_common = new String();
		int least_common_count = 20000;
		List<String> list = text;
		Collections.sort(list);
		String current = list.get(0);
		int current_count = 0;
		for (String word: list) {
			if (word.equals(current)){
				current_count++;
			}
			else {
				if (current_count < least_common_count){
					least_common = current;
					least_common_count = current_count;
				}
				current = word;
				current_count = 1;
			}
		}
		return least_common + " - " + least_common_count;
	}
	
	public static String most_common(ArrayList<String> text) {
		String most_common = new String();
		int most_common_count = 0;
		List<String> list = text;
		Collections.sort(list);
		String current = list.get(0);
		int current_count = 0;
		for (String word: list) {
			if (word.equals(current)){
				current_count++;
			}
			else {
				if (current_count > most_common_count){
					most_common = current;
					most_common_count = current_count;
				}
				current = word;
				current_count = 1;
			}
		}
		return most_common + " - " + most_common_count;
	}
	
	public static int count_unique_words(int auth_index, ArrayList<ArrayList<String>> text) {
		int count = 1;
		List<String> list = text.get(auth_index);
		Collections.sort(list);
		String current = list.get(0);
		for (String word: list){
			if (word.equals(current)) {
				current = word;
				count++;
			}
		}
		return count;
	}
	
	public static void add_text(int auth_index, String book_text, ArrayList<ArrayList<String>> text) {
		// For new authors, create a new vector of words
		if (text.size() <= auth_index) {
			text.add(new ArrayList<String>());
			assert (text.size() > auth_index);
		}
		
		// Cut up the current line
		book_text = book_text.toLowerCase();
		String current = new String();
		for (int c = 0; c < book_text.length(); c++){
			if (book_text.charAt(c) >= 'a' && book_text.charAt(c) <= 'z') {
				current += book_text.charAt(c);
			}
			else if (current.length() > 0) {
				text.get(auth_index).add(current);
				current = new String();
			}
		}
	}
	
	public static int increment_author(String author, ArrayList<String> authors) {
		boolean found = false;
		for (int i = authors.size()-1; i >= 0; i--) {
			if (authors.get(i).equals(author)) {
				found = true;
				return i;
			}
		}
		if (!found) {
			authors.add(author);
			return (authors.size()-1);
		}
		return -1;
	}

}
