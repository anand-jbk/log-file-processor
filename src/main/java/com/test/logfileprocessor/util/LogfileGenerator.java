package com.test.logfileprocessor.util;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * To generate sample log entries for testing
 * @author anand
 *
 */
public class LogfileGenerator {

	public static void main(String[] args) throws IOException {
		
		String path = "C:/Users/Public/Work/workspace-spring-tool-suite-4-4.10.0.RELEASE/log_100000000.txt";
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		String st = "{\"id\":\"%s\", \"state\":\"STARTED\", \"timestamp\":%d,  \"type\":\"APPLICATION_LOG\", \"host\":\"12345\"}";
		String ft = "{\"id\":\"%s\", \"state\":\"FINISHED\", \"timestamp\":%d,  \"type\":\"APPLICATION_LOG\", \"host\":\"12345\"}";
		
		int l1 = 10000;
		int l2 = 10000;
		
		int id = 0;
		for(int j=0; j< l1; j++) {
			Set<String> logs = new HashSet<String>();
			for (int i = 0; i < l2; i++) {
				logs.add(String.format(st, id, id));
				id++;
			}
			
			for (Iterator iterator = logs.iterator(); iterator.hasNext();) {
				String string = (String) iterator.next();
				writer.write(string);
				writer.write("\n");
			}	
			logs.clear();
		}
		
		id = 0;
		for(int j=0; j< l1; j++) {
			Set<String> logs = new HashSet<String>();
			for (int i = 0; i < l2; i++) {
				logs.add(String.format(ft, id,2*id));
				id++;
			}
			
			for (Iterator iterator = logs.iterator(); iterator.hasNext();) {
				String string = (String) iterator.next();
				writer.write(string);
				writer.write("\n");
			}	
			logs.clear();
		}
		
		
		writer.close();
	}

}
