package mutual_friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class MutualFriends {
	
	public static void main(String[] args) throws Exception {
	
		if (args.length != 2) {
            System.err.println("Usage: MutuaFriends <in_dir> <out_dir>");
            System.exit(2);
        }
		
		Configuration conf = new Configuration();		
		Job job = new Job(conf, "MutualFriends");
		
		job.setJarByClass(MutualFriends.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	    job.waitForCompletion(true);
	    
	}
	
}
