package generated;
import redis.clients.jedis.Jedis;
public class Main {
  public static void main(String[] args) {
    Jedis redis = new Jedis("localhost", 6379);
    String[] records0 = redis.keys("Student:*").toArray(new String[0]);
    String[] records1 = redis.keys("Grade:*").toArray(new String[0]);
    for(String table0 : records0){
      for(String table1 : records1){
        if(redis.hget(table0,"ID").compareTo(redis.hget(table1,"ID"))==0&&(Integer.valueOf(redis.hget(table0,"Age")).compareTo(42)>0||redis.hget(table0,"FName").compareTo("Maria")==0)){
           System.out.println(redis.hget(table0,"FName")+", "+ redis.hget(table0,"LName")+", "+ redis.hget(table1,"ID")+", "+ redis.hget(table1,"Mark"));
        }
      }
    }
  }
}
