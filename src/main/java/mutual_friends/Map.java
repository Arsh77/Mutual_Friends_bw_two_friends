package mutual_friends;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text word = new Text();
    private Text friendList = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String[] splitArr = value.toString().split("\t", -1);
        
    	if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
        	return;
        }
        
    	String[] friendList1 = splitArr[1].split(",");
    	int[] friendList2 = new int[friendList1.length];
    	
    	for(int i = 0; i < friendList1.length ; i++) {
    		friendList2[i]=Integer.parseInt(friendList1[i]);
    	}
    	
    	Arrays.sort(friendList2);
    	
    	String friendList3 = "";
    	for(int a : friendList2) {
    		friendList3+=a+",";
    	}
    	
    	Text friendVal = new Text(friendList3);
    	
    	int user = Integer.parseInt(splitArr[0]);
    	
    	for(int a : friendList2) {
    		String friendKeyPair="";
    		if(user<a) {
    			friendKeyPair+=user+","+a;
    		}
    		else if(user>a) {
    			friendKeyPair+=a+","+user;
    		}
    		context.write(new Text(friendKeyPair), friendVal);
    	}
		
    }
}
