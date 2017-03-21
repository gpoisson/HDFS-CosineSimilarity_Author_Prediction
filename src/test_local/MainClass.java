package test_local;

import java.io.BufferedReader;
import java.io.FileReader;

public class MainClass {
	
	public static void main(String[] args) {
	
		String textFile = "testData.txt";
		BufferedReader reader;
		
		try {
			reader = new BufferedReader(new FileReader(textFile));
		} catch (Exception e) {
			
		}
	}

}
