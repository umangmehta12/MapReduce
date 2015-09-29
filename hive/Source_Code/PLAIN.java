
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.*;



public class flightDetails {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();
		private String year,month,flightDate,origin,destination,ArrDelayMinutes,
		cancelled,diverted,ArrTime,DepTime;  
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			CSVParser parser = new CSVParser();
			String[] lines = parser.parseLine(value.toString());
			year=lines[0];
			month=lines[2];
			flightDate=lines[5];
			origin=lines[11];
			destination=lines[17];
			DepTime=lines[24];
			ArrTime=lines[35];
			ArrDelayMinutes=lines[37];
			cancelled=lines[41];

			if((Integer.parseInt(year) == 2007 && Integer.parseInt(month) >= 6) 
					|| (Integer.parseInt(year) == 2008 && Integer.parseInt(month) <= 5))
			{
				if(!(year.equals(""))&&(!month.equals(""))&&(!flightDate.equals(""))&&(!origin.equals(""))
						&&(!destination.equals(""))&&(!DepTime.equals(""))&&(!ArrTime.equals(""))&&
						(!ArrDelayMinutes.equals("")))
				{
					if(origin.equals("ORD")&& !(destination.equals("JFK")))
					{
						StringBuilder concat=new StringBuilder();
						concat.append(flightDate);
						concat.append(",");
						concat.append(origin);
						concat.append(",");
						concat.append(destination);
						concat.append(",");
						concat.append(ArrTime);
						concat.append(",");
						concat.append(DepTime);
						concat.append(",");
						concat.append(ArrDelayMinutes);
						concat.append(",");
						concat.append("LegOne");
						Text Leg1= new Text(concat.toString());
						Text destinationtext= new Text(destination);
						context.write(destinationtext,Leg1);
					}
					else if(!(origin.equals("ORD"))&& (destination.equals("JFK")))
					{
						StringBuilder concat=new StringBuilder();
						concat.append(flightDate);
						concat.append(",");
						concat.append(origin);
						concat.append(",");
						concat.append(destination);
						concat.append(",");
						concat.append(ArrTime);
						concat.append(",");
						concat.append(DepTime);
						concat.append(",");
						concat.append(ArrDelayMinutes);
						concat.append(",");
						concat.append("LegTwo");
						Text Leg2= new Text(concat.toString());
						Text origintext= new Text(origin);
						context.write(origintext,Leg2);
					} 
				}
			}
		}
	} 

	public static class FlightReducer 
	extends Reducer<Text,Text,Text,Text> {		

		private ArrayList<Text> LegOne = new ArrayList<Text>();
		private ArrayList<Text> LegTwo = new ArrayList<Text>();
		private final static Text one = new Text("1");

		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			
           // Clearing the Array List
			LegOne.clear();
			LegTwo.clear();
			
		   // Iterating over all the values and spliting it into 
		   // String [] while checking for dummy value passed and then
		   // appending to the Array List
			for(Text t : values)
			{  String[] splitString = t.toString().split(",");
			if(splitString[6].equals("LegOne"))
			{
				LegOne.add(new Text (t.toString()));
			}
			else if(splitString[6].equals("LegTwo"))
			{

				LegTwo.add(new Text(t.toString()));
			}
			}

			//Iterating over all the values of arrayList for LegOne and
			//checking for a match and condition in ArrayList LegTwo
			// if it satisfies the conditions emittiong it.
			for(Text first:LegOne)
			{
				String[] legOneObject = first.toString().split(",");

				for(Text second: LegTwo)
				{   
					String[] legTwoObject = second.toString().split(",");
					if((legOneObject[0].equals(legTwoObject[0]))
							&&((Double.parseDouble(legTwoObject[4]))>(Double.parseDouble(legOneObject[3]))))
					{ 
						double TotalDelay=(Double.parseDouble(legOneObject[5])+Double.parseDouble(legTwoObject[5]));
						context.write(one, new Text(TotalDelay+""));
					}
				}
			}
		}
	}
	
	public static class CalculateDelayMapper 
	extends Mapper<Object, Text, Text, Text>{ 
		private int countofflights;
		private double total_delay_time;

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			countofflights++;
			String[] values = value.toString().split("\\t");
			total_delay_time += Double.parseDouble(values[1]);

		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text("1"),new Text(countofflights+","+total_delay_time));
		}

	}


	public static class AverageReducer 
	extends Reducer<Text,Text,Text,Text> {


		private double countofflights;
		private double flight_total_delay;

		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {


			for (Text val : values) {
				String[] value_list = val.toString().split(",");
				countofflights += Double.parseDouble(value_list[0]);
				flight_total_delay += Double.parseDouble(value_list[1]);
			}
			context.write(new Text("Average Delay of all Flights:"), new Text((flight_total_delay/countofflights)+""));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Flight details Job1");
		job.setJarByClass(flightDetails.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(FlightReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);

		//Job to find the average of the Arrival delay from Reduce Tasks

		Job job1 = new Job(conf, "Flight details Job2");
		job1.setJarByClass(flightDetails.class);
		job1.setMapperClass(CalculateDelayMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(AverageReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		job1.waitForCompletion(true);	
	}
}