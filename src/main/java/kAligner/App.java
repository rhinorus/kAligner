package kAligner;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class App {
    private App() {
    }

    static Connection   connection  = null;
    static Statement    statement   = null;

    static String 
                    referenceFilePath   = null,
                    sequence            = null;

    static Integer kmerSize = 12;

    public static void main(String[] args) {

        try {
            connection = getConnection(); 
            statement = connection.createStatement();
        } catch (Exception e) {
            System.out.println("Cannot connect to database.");
            System.out.println(e);
            return;
        }

        try{
            createTables();
        }
        catch(Exception ex){
            System.out.println("Cannot create one or more tables.");
            System.out.println(ex);
            return;
        }

        insertValue("TTTCCCGGGTTT", 0);

        if(isIndexOnKmerSizeExists()){
            System.out.println("index on kmer size: " + kmerSize + " exists.");
        }
        else{
            System.out.println("index on kmer size: " + kmerSize + " doesn't exists.");
        }

         
    }  

	public static Connection getConnection() throws ClassNotFoundException, SQLException 
    {
        File indexesFolder = new File("indexes");
        if(!indexesFolder.exists())
            indexesFolder.mkdir();

        Class.forName("org.sqlite.JDBC"); 
        return DriverManager.getConnection("jdbc:sqlite:indexes/database.s3db");
    }
 
    public static void createTables() throws Exception {
        String kmers =  "CREATE TABLE if not exists kmers ("  +
                        "sequence VARCHAR PRIMARY KEY, "    +
                        "kmer_size INTEGER NOT NULL ); ";

        try{
            statement.execute(kmers);
        }
        catch(SQLException ex){
            if(ex.getSQLState() != "SQLITE_DONE")
                throw new Exception(ex.getMessage());
        }
        
        String positions =  "CREATE TABLE if not exists 'positions' ("            +
                            "kmer INTEGER, " +
                            "position INTEGER NOT NULL, " +
                            "CONSTRAINT fk_kmers FOREIGN KEY(kmer) REFERENCES kmers(sequence));"; 

        try{
            statement.execute(positions);
        }
        catch(SQLException ex){
            if(ex.getSQLState() != "SQLITE_DONE")
                throw new Exception(ex.getMessage());
        }
    }
 
    public static boolean isIndexOnKmerSizeExists(){
        String query = "SELECT * FROM kmers WHERE kmer_size = " + kmerSize + " LIMIT 1; ";

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

    public static boolean isKmerExists(String kmer){
        String query = "SELECT * FROM kmers WHERE sequence = '" + kmer + "' LIMIT 1";
        
        try {
            ResultSet result = statement.executeQuery(query);
            String foundedKmer = result.getString("sequence"); 

            if(foundedKmer.equals(kmer))
                return true;
            return false;
        } catch (SQLException e) {
            return false;
        }

    }

    public static void insertValue(String kmer, Integer position){

        if(!isKmerExists(kmer)){
            String query =  "INSERT INTO 'kmers' ('sequence', 'kmer_size') " +
                            "VALUES ('" + kmer + "', " + kmerSize + ");";
            
            try {
                statement.execute(query);
            } catch (SQLException e) {
                System.out.println("Cannot insert kmer.");
                System.out.println(e.getMessage());
                return;
            }
        }

        String query =  "INSERT INTO 'positions' ('kmer', 'position') " +
                        "VALUES('" + kmer + "', " + position + ");";

        try {
            statement.execute(query);
        } catch (SQLException e) {
            System.out.println("Cannot insert kmer position.");
            System.out.println(e.getMessage());
            return;
        }           
    }
} 
