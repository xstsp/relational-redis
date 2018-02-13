package redis;

import java.io.*;

public class Main {
    public static void main(String[] args) throws FileNotFoundException, IOException{
		Redis redis = new Redis();
		//redis.readSchemaFile(args[0]);
		redis.readQueryFile(args[1]);
  }
}
