import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class A_Final_MainClass {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "MainClass");
        job.setJarByClass(A_Final_MainClass.class);
         
	    job.setMapperClass(A_Final_MapClass.class);
//	    job.setCombinerClass(Final_ReduceClass.class);
	    job.setReducerClass(A_Final_ReduceClass.class);
	    
	    job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
