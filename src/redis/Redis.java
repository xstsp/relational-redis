package redis;

import java.util.*;
import redis.clients.jedis.Jedis;
import java.io.*;

public class Redis {

    private BufferedReader file;
    private ArrayList<String> schema;
    
    Redis() {
        schema = new ArrayList<String>();
    }
  
    public void readSchemaFile(String filename) throws FileNotFoundException, IOException {

        String curLine;

        try {
        	file = new BufferedReader(new FileReader(filename));
	    }
	    catch(FileNotFoundException ex) {
	    	System.out.println("Check schema file! (not found)");
	        return;        	
	    }
        /* Read table name */
        schema.add(file.readLine());

        /* Read schema */
        while ( !(curLine = file.readLine()).equals(";") ) {
            schema.add(curLine);
        }
        for (String sc : schema) {
            System.out.println(sc);
        }
        mapSchemaToRedis();
    }
  
    public void mapSchemaToRedis() throws IOException {

        String curLine;
        String[] records;
        String tableName  = schema.get(0);
        Jedis redis = connectToRedis();
        
        while ((curLine = file.readLine()) != null) {
            records = curLine.split(";");
            /* Mapping to Redis */
            String hashName = tableName + ":" + records[0];
            for (int i=0 ; i<records.length ; i++)
            {
                redis.hset(hashName, schema.get(i+1), records[i]);
            }
        }
    }
        
    public void readQueryFile(String filename) throws FileNotFoundException, IOException {

        ArrayList<OutputRelation> relations = new ArrayList<OutputRelation>();           //contains the relations of the query
        String curLine;
        String[] tableRecords;

        try {
        	file = new BufferedReader(new FileReader(filename));
        }
        catch(FileNotFoundException ex) {
        	System.out.println("Check query file! (not found)");
            return;        	
        }

        Jedis redis = connectToRedis();
        
        /* Read "SELECT" */
        curLine = file.readLine().replaceAll("\\s","");
        String[] select = curLine.split(",");

        /* Read "FROM" */
        curLine = file.readLine().replaceAll("\\s","");
        String[] from = curLine.split(",");
        
        for (String relation : from) {

            tableRecords = redis.keys(relation + ":*").toArray(new String[0]);                 // >KEYS Student:*
            
            if ( tableRecords.length == 0 )
            {
                System.out.println("Relation " + relation + " does not exist");
                return;
            }
            
            OutputRelation rel = new OutputRelation();
            rel.name = relation;         
            
            String [] tableFields = redis.hkeys(tableRecords[0]).toArray(new String[0]);

            for (String field : tableFields) {
            	boolean found = true;
            	Field newField = new Field();

	            for (String selectField : select) {
	                String[] splitSelection = selectField.split("\\.");
	                
	                newField.name = field;
	                if (relation.equals(splitSelection[0])) {                                 // if field belongs to the current relation
	                    if (field.equals(splitSelection[1])) {                                // if field belongs to this hash (relation)
	                    	newField.isSelect = true;                                         // keep it for output                          
	                        found = true;
	                        break;
	                    }
	                }
	            }
	            if (!found) {
                    System.out.println("Field " + field + " does not exist in relation " + relation);
                    return;
                }
                rel.outputFields.add(newField); 
            }
            relations.add(rel);                                                               // keep the fields to be printed
        }
        
        /*  Read "WHERE" */
        String whereCondition;
        if ( (whereCondition = tokenizeWhere(file.readLine(), relations)) == null ) {
            return;
        }
        
        /* A new java file will be generated with code to return the proper records from Redis, based on the query */

        String genCode ="";
        genCode += "package generated;\n";
        genCode += "import redis.clients.jedis.Jedis;\n";
        genCode += "public class Main {\n";
        genCode += "  public static void main(String[] args) {\n";
        genCode += "    Jedis redis = new Jedis(\"localhost\", 6379);\n";
        
        int tableCount = 0;
        for (String relation : from) {
            genCode += "    String[] records" + tableCount + " = redis.keys(\"" + relation + ":*\").toArray(new String[0]);";
            genCode += "\n";
            tableCount++;
        }
        
        for (tableCount=0 ; tableCount < from.length ; tableCount++) {
            genCode += "    for(String table" + tableCount + " : records" + tableCount + "){";
            genCode += "\n  ";
        }
        
        genCode += "      if(" + whereCondition + "){\n    ";
        genCode += "       System.out.println(";
        
        for (int i=0; i<relations.size(); i++) {

        	ArrayList<Field> outFields = relations.get(i).outputFields;
            for( int f=0 ; f<outFields.size() ; f++) {

            	if (outFields.get(f).isSelect) {
	                genCode += "redis.hget(table" + i + ",\"" + outFields.get(f).name + "\")";
	                if ( i != relations.size()-1 || f != outFields.size()-1 )
	                    genCode += "+\", \"+ ";
            	}
            }
        }
        genCode += ");";
        genCode += "\n        }";
        genCode += "\n      }";
        genCode += "\n    }";
        genCode += "\n  }";
        genCode += "\n}";        
        
        System.out.println(genCode);

        /* write generated code to file */
        try (PrintWriter out = new PrintWriter("src/generated/Main.java")) {
            out.println(genCode);
        }
        catch(IOException ex) {
        	System.out.println("Cannot write generated super code to file :(");
            return;        	
        }
    }
    
    public String tokenizeWhere(String line, ArrayList<OutputRelation> relations) {
        
        /* replace operators with our dummy values */
        line = line.replaceAll("\\s","");
        line = line.replace("<>","!");
        line = line.replace("AND","&");
        line = line.replace("OR","|");

        String[] clauses = line.split("&|\\|");      // split into clauses (delimiters , pattern: fieldName operand value
                                                           
        short table = 0, left = 0;
        short field = 1, right = 1;
        
        for (String condition : clauses) {
            condition = condition.replace("(", "");
            condition = condition.replace(")","");
            
            String[] clause = condition.split("=|>|<|!");
            
            String oldCondition = condition;
            System.out.println("old condition: " + oldCondition);
            
            boolean found = false;

            /* Right part */
            String[] tokens = clause[right].split("\\.");
            
            if (tokens.length == 1) {                   // literals, not table fields
                if(clause[right].matches("[0-9]+"))     // number
                    condition = condition.replace(clause[left], "Integer.valueOf(" + clause[left] + ").compareTo(" + tokens[0] + ")");
                else {                                  // string
                    condition = condition.replace(clause[left], clause[left] + ".compareTo(\"" + tokens[0] + "\")");
                }
                condition = condition.replaceAll(clause[right] + "$", "0");  // replace only the number before the end of line
            }
            else {
            	/* check if table exists in db */
                for (int t=0 ; t<relations.size() ; t++) {
                	
                	OutputRelation curRel = relations.get(t);

                    /* this is done to map the table number for redis key */
                    if (tokens[table].equals(curRel.name)) {                     
                    	
                        /* check if field exists in db for this table */
                    	for (int f=0 ; f<curRel.outputFields.size() ; f++) {
                    		
                    		ArrayList<Field> relFields = curRel.outputFields;
                    		if (tokens[field].equals(relFields.get(f).name)){

		                        String temp = "redis.hget(table" + t + ",\"" + tokens[field] + "\")";
		                        condition = condition.replace(clause[right], temp);
		                        condition = condition.replace(clause[left], clause[left] + ".compareTo(" + temp + ")");

		                        if (condition.contains("!"))    // <> symbol
		                            condition = condition.replaceAll("([!][\\S]+)$", "!0");
		                        else
		                            condition = condition.replaceAll("([=<>][\\S]+)$", "=0");
		                        found = true;
		                        break;
                    		}
                    	}
                    }
                }
                /* wrong query */
                if (!found) {      
                    System.out.println("Error in WHERE condition: Something is wrong with " + clause[right]);
                    return null;
                }
            }

            /* Left part */
            tokens = clause[left].split("\\.");

            found = false;
            for (int t=0 ; t<relations.size() ; t++) {
            	
            	OutputRelation curRel = relations.get(t);

                /* this is done to map the table number for redis key */
                if (tokens[table].equals(curRel.name.trim())) {

                	/* check if field exists in db for this table */
                	for (int f=0 ; f<curRel.outputFields.size() ; f++) {
                		
                		ArrayList<Field> relFields = curRel.outputFields;
                		if (tokens[field].equals(relFields.get(f).name)) {
		                    condition = condition.replace(clause[left], "redis.hget(table" + t + ",\"" + tokens[field] + "\")");
		                    found = true;
		                    break;
                		}
                	}
                }
            }
            /* wrong query */
            if (!found) {      
                System.out.println("Error in WHERE condition: Something is wrong with " + clause[left]);
                return null;
            }
            System.out.println("new condition: " + condition);
            line = line.replace(oldCondition, condition);
        }
        
        line = line.replace("=","==");
        line = line.replace("!","!=");
        line = line.replace("&","&&");
        line = line.replace("|","||");
        return line;
    }

    public Jedis connectToRedis() {
        Jedis jedis = new Jedis("localhost", 6379);
        return jedis;
    }
    
    public class OutputRelation {
        String name;
        ArrayList<Field> outputFields;
        
        OutputRelation() {
            outputFields = new ArrayList<Field>(); //contains the relation's fields that should be printed
        }
        
    }

    public class Field {
        String name = null;
        boolean isSelect = false;
    }
    
}