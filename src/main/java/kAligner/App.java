package kAligner;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import kAligner.classes.FastaFile;
import kAligner.classes.SequenceKmer;

public final class App {
    private App() {
    }

    static Connection connection = null;
    static Statement statement = null;

    static String   referenceFilePath = "example/reference.fa", 
                    sequence =  "TCCTGTCCCTGCCCACCGCTTTAAGAAACTGTTAGTTCTGTTTGAAATGT" +
                                "CATCACTGGGCGTGAATTCCCATCTTCAGATAATCCCGCCAGGCGGTGTT" +
                                "TGGATGAGGAGTTTATTCAGGCTGTGGGGGTCTGTGCGTGCCCGCGGGTC" +
                                "CCTCCCACTGCAGCCGGCGAGGCTGGCCCTGACCCAGAGTCGCCCGCCCA" +
                                "CCCAGTCTACGTAGGCATTAGAGGTTTGGGTTCTGAGCTCAGGGCCTGGT";   
    static Integer kmerSize = 12; 
    static Double minSearchValue = 0.15;
 
    static FastaFile reference = null; 

    static final DateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    static final boolean    DEBUG_MODE = true;

    static Set<String> checkedKmers = new HashSet<>();
 
    public static void main(String[] args) {

        reference = new FastaFile(referenceFilePath);

        try {
            connection = getConnection(); 
            statement = connection.createStatement();
        } catch (Exception e) {
            System.out.println("Cannot connect to database.");
            System.out.println(e);
            return;
        }
 
        // try{
        //     createTable();
        // }
        // catch(Exception ex){
        //     System.out.println("Cannot create one or more tables.");
        //     System.out.println(ex);
        //     return;
        // } 
        
        // buildIndex();  
 
        lookForSequence();
    }  

	public static Connection getConnection() throws ClassNotFoundException, SQLException 
    {
        File indexesFolder = new File("indexes");
        if(!indexesFolder.exists())
            indexesFolder.mkdir();

        Class.forName("org.sqlite.JDBC"); 
        return DriverManager.getConnection("jdbc:sqlite:indexes/" + reference.getFileName());
    }
   
    public static void createTable() throws Exception {

        String positions =  "CREATE TABLE if not exists 'positions' ("            +
                            "kmer INTEGER, " +
                            "position INTEGER," +
                            "kmer_size INTEGER ); "; 

        try{
            statement.execute(positions);
        }
        catch(SQLException ex){
            if(ex.getSQLState() != "SQLITE_DONE")
                throw new Exception(ex.getMessage());
        }

        String positionsIndex = "CREATE INDEX kmer_index ON positions(kmer)";
        statement.execute(positionsIndex); 
    }
   
    public static boolean isIndexOnKmerSizeExists(){
        String query = "SELECT * FROM positions WHERE kmer_size = " + kmerSize + " LIMIT 1; ";

        try {
            ResultSet result = statement.executeQuery(query);
            Integer foundedKmerSize = result.getInt("kmer_size");
 
            if(foundedKmerSize.equals(kmerSize))
                return true; 
            return false;

        } catch (SQLException e) {
            return false;
        }
    }
  
    public static void insertNotExistingKmers(Iterable<String> kmers) throws SQLException {

        ArrayList<String> filteredKmers = new ArrayList<>();
        for(String kmer : kmers)
            if(!checkedKmers.contains(kmer))
                filteredKmers.add(kmer);

        String query =  "SELECT sequence FROM kmers " +
                        "WHERE sequence IN ('" + String.join("', '", filteredKmers) + "'); ";

        Set<String> existingKmers = new HashSet<>();   
        
        ResultSet result = statement.executeQuery(query);
        String kmer = null;

        if(!result.isClosed()){
            kmer = result.getString("sequence"); 
            existingKmers.add(kmer);
        } 
        
        while(!result.isClosed()){
            result.next();
            if(!result.isClosed()){
                kmer = result.getString("sequence");
                existingKmers.add(kmer);
            }
        }
 
        ArrayList<String> kmersForInsert = new ArrayList<>();
        for(String value : filteredKmers)
            if(!existingKmers.contains(value))
                kmersForInsert.add(value);

        if(kmersForInsert.size() > 0){
            String insertQuery =    "INSERT INTO kmers ('sequence', 'kmer_size') " +
                                    "VALUES ('";

            String values = String.join("', " + kmerSize + "), ('", kmersForInsert);
 
            insertQuery += values + "', " + kmerSize + "); "; // last value
            statement.execute(insertQuery);  

            for(String value : kmersForInsert)
                checkedKmers.add(value); 
        }
    }

    public static void insertMany(HashMap<String, ArrayList<Integer>> values) throws SQLException {
        String insertQuery =    "INSERT INTO positions('kmer', position) VALUES ('";

        StringBuilder builder = new StringBuilder();

        for(String kmer : values.keySet()){
            for(Integer position : values.get(kmer)){
                builder.append(kmer + "', " + position + "), ('");
            }
        }

        builder.delete(builder.length() - 4, builder.length());
        builder.append(";");

        insertQuery += builder.toString();
        statement.execute(insertQuery);
    }

    public static void buildIndex(){
         
        if(isIndexOnKmerSizeExists()){
            System.out.println("index on kmer size: " + kmerSize + " exists.");
        }
        else{
            try {
                reference.readFile();
            } catch (IOException e) {
                System.out.println("Cannot read reference file.");
                e.printStackTrace();
                return;
            }
            
            printLog("Starting build index...");

            int count = 0;
            int windowSize = 1 * 1000 * 1000;

            HashMap<String, ArrayList<Integer>> values = new HashMap<>();

            for(int i = 0; i < reference.getSequenceChars().length - kmerSize; i++){
                StringBuilder kmer = new StringBuilder();

                for(int j = 0; j < kmerSize; j++)
                    kmer.append(reference.getSequenceChars()[i + j]);

                if(!values.containsKey(kmer.toString()))
                    values.put(kmer.toString(), new ArrayList<>());
                
                values.get(kmer.toString()).add(i);
                count++;
  
                if(count == windowSize){
                    try {
                        insertMany(values);
                        printLog("Processed " + (i + 1) + " bases of " + reference.getSequenceChars().length);
                    } catch (SQLException e) {
                        System.out.println("Cannot insert index values.");
                        e.printStackTrace();
                        return;
                    }
                    values.clear(); 
                    count = 0;
                }
            }   

            if(count > 0){
                try {
                    insertMany(values);
                } catch (SQLException e) {
                    System.out.println("Cannot insert index values.");
                    e.printStackTrace();
                    return;
                }
            }
            
            System.out.println("Indexing on kmer size: " + kmerSize +" done.");
        }
    }
     
    public static void lookForSequence(){
        printLog("Starting sequence search.");
        char[] chars = sequence.toUpperCase().toCharArray();

        Set<SequenceKmer> sequenceKmers = new HashSet<>();  

        for(int i = 0; i < chars.length - kmerSize; i++){
            StringBuilder kmer = new StringBuilder();
            
            for(int j = 0; j < kmerSize; j++)
                kmer.append(chars[i + j]);

            SequenceKmer sequenceKmer = new SequenceKmer(kmer.toString(), i);
            sequenceKmers.add(sequenceKmer);    
            
            try {
                ArrayList<Integer> positions = getKmerPositions(sequenceKmer.getSequence());
                sequenceKmer.addPositions(positions);
            } catch (SQLException e) {
                printLog("Cannot find positions for kmer: " + sequenceKmer.getSequence());
                e.printStackTrace();
                return;
            }
        } 

        HashMap<Integer, Integer> positionsValues = new HashMap<>();
        for(SequenceKmer sequenceKmer : sequenceKmers){
            for(Integer referencePosition : sequenceKmer.getReferencePositions()){
                int calculatedPosition = referencePosition - sequenceKmer.getPosition();

                if(positionsValues.containsKey(calculatedPosition)){
                    int current = positionsValues.get(calculatedPosition);
                    positionsValues.put(calculatedPosition, current + 1);
                }
                else
                    positionsValues.put(calculatedPosition, 1);
            }
        }

        Double minValue = (chars.length - kmerSize) * minSearchValue;
        Integer minIntValue = minValue.intValue();
 
        for(Integer key : positionsValues.keySet()){ 
            if(positionsValues.get(key) > minIntValue)
                System.out.println("position: " + key + ": " + getPercentage(positionsValues.get(key), chars.length - kmerSize) + "%");
        }
    } 
 
    public static ArrayList<Integer> getKmerPositions(String kmerSequence) throws SQLException {

        String query = "SELECT position FROM positions WHERE kmer = '" + kmerSequence + "';";
        ResultSet result = statement.executeQuery(query);

        ArrayList<Integer> positions = new ArrayList<>();

        while(!result.isClosed()){
            Integer position = result.getInt("position");
            positions.add(position);
            result.next();
        }

        return positions;
    }
 
    private static String getPercentage(Integer value, Integer total){
        Double doublePercentage = (double)value / total * 100.0;
        return String.format("%.2f", doublePercentage);
    }

    /**
     * Logs message with timestamp.
     */
    private static void printLog(String message){
        if(DEBUG_MODE){
            String time = sdf.format(new Date()); 
            System.out.println(time + ": " + message);
        }
    }
 
}    
