package kAligner.classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FastaFile {

    private String  filePath,
                    fileName;
    
    private char[]  sequence; 
 
    public FastaFile(String filePath) {
        this.filePath = filePath;

        String[] options = filePath.split("/");
        fileName = options[options.length - 1]; 
    }

    public String getFileName(){
        return fileName;
    }

    public char[] getSequenceChars(){
        return sequence;
    }

    public void readFile() throws IOException {
        System.out.println("Starting " + fileName + " file reading.");

        File file = new File(filePath);

        StringBuilder builder = new StringBuilder();

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();

        while(line != null){
            if(line.contains(">")){
                line = reader.readLine();
                continue;
            }
                

            builder.append(line);
            line = reader.readLine();
        }

        reader.close();
        sequence = builder.toString().toUpperCase().toCharArray();
        System.out.println("done.");
    }
}  