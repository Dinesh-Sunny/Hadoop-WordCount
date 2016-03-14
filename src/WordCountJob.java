import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJob implements Tool{
	
	//initializing the configuration object
	private Configuration conf;
	
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		//Initialize the job object with configuration
		Job job = new Job(getConf());
		job.setJobName("Hadoop");
		
		//detecting the main job class
		job.setJarByClass(this.getClass());
		
		//setting the mapper
		job.setMapperClass(WordCountMapper.class);
		//setting the reducer class
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class); //key2
		job.setMapOutputValueClass(LongWritable.class);//value2
		
		job.setOutputKeyClass(Text.class); //key3
		job.setOutputValueClass(LongWritable.class);//value3

		job.setInputFormatClass(TextInputFormat.class); //setting import format class
		job.setOutputFormatClass(TextOutputFormat.class);//setting output format
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// input file
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//output folder
		return job.waitForCompletion(true) ? 0 : -1; //execute and give result
	}
	
	public static void main(String[] args) throws Exception{
		int status = ToolRunner.run(new Configuration(), new WordCountJob(), args);
		System.out.print("Status: " + status);
	}

}
