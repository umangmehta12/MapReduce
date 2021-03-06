package permap;



///**
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

/*
The condition to check the counts for the real word.I have developed a RegularExpression(Regex)
 to determine the counts of the “real” words starting with m,n,o,p,q,M,N,O,P,Q
*/
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			HashMap<String,Integer> hmap= new HashMap<String,Integer>();
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String wordToString=word.toString();
				if(Pattern.matches("[m-qM-Q].*$", wordToString))
				{if(hmap.containsKey(wordToString))
					hmap.put(wordToString, hmap.get(wordToString)+1);
				else
					hmap.put(wordToString, 1);
				}
			}
			Iterator it = hmap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String,Integer> keyValuePair = (Map.Entry<String,Integer>)it.next();
				IntWritable valueaccepted = new IntWritable(keyValuePair.getValue());
				Text texttoremove= new Text(keyValuePair.getKey());
				context.write(texttoremove, valueaccepted);
				  it.remove(); // avoids a ConcurrentModificationException

			}


		}
		
	}

/*A custom Partioner that assigns words that start with a specific alphabet to the given Reduced task.
The problem statement says the reduce task 0 should contain words that start either with m or M.
I have implemented the same approach.I have done so by taking the CharAt(0) the first position of 
the value converted toString() and then checking against the accepted values*/
	
	  public static class WordPartitioner extends Partitioner <Text, IntWritable> {
	      @Override
	      public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	
	          String val = key.toString();
	          if(val.charAt(0)=='m'||val.charAt(0)=='M')
	        	  return 0;
	          
	          if(val.charAt(0)=='n'||val.charAt(0)=='N')
	        	  return 1;
	          
	          if(val.charAt(0)=='o'||val.charAt(0)=='O')
	        	  return 2;
	          
	          if(val.charAt(0)=='p'||val.charAt(0)=='P')
	        	  return 3;
	          
	          if(val.charAt(0)=='q'||val.charAt(0)=='Q')
	        	  return 4;
	          
	          else
	        	  return -1;
	   
	          }
	  }
	
	public static class IntSumReducer 
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(WordPartitioner.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}

