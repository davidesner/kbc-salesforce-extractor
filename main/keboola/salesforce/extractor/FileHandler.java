package keboola.salesforce.extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
 
 
public class FileHandler
{       
    public static void writeCSVFromStream(InputStream in, String object, String filesDirectory) throws IOException{
        //gets QueryResultStream from BulkExample and object name, creates BufferedReader and output file in specified folder
        //makes output folder if it doesnt exist
        BufferedReader read = new BufferedReader(new InputStreamReader(in));
        //writes csv file to output folder

        FileWriter csvFile = new FileWriter(filesDirectory+object+".csv");
        String line = "";
        CsvWriter writer = new CsvWriter(csvFile);
        //makes array of results
        while((line = read.readLine()) != null){
                if(line.length()>0){
                        line.trim();
                        String[] row = line.split("___");
                        writer.writeRecord(row);
                }
           
        }
        //closes stream
   		System.out.println( "Close file");

        writer.endDocument();
    }
}