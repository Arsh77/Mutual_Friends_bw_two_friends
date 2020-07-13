package mutual_friends;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		
		String[] groupedList = new String[2];
    	String[][] friendList = new String[2][];
    	ArrayList<String> resultList = new ArrayList<>();
    	
    	int index = 0, i = 0, j = 0;
    	for (Text val : values) {
    		groupedList[index] = val.toString();
    		friendList[index] = groupedList[index].split(",");
    		index++;
    	}
    	
    	while (index == 2 && i < friendList[0].length && j < friendList[1].length) {
    		if (friendList[0][i].equals(friendList[1][j])) {
    			resultList.add(friendList[0][i]);
    			i++;
    			j++;
    		} else if (Integer.parseInt(friendList[0][i]) > Integer.parseInt(friendList[1][j])) {
    			j++;
    		} else {
    			i++;
    		}
    	}
    	
    	String result = "";
    	if (resultList.isEmpty()) {
    		result = "no_mutual_friends";
    	} else {
    		result = String.join(",", resultList);
    	}
    	
    	context.write(key, new Text(result));
	}
}
