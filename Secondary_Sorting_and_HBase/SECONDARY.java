
public class flightdetails {

        //OutputKey is an IntPair which has the AirlineID and Flight month
        //OutputValue is DoubleWritable which is the Arrival Delay for each flight
	public static class TokenizerMapper 
	extends Mapper<Object, Text, IntPair, DoubleWritable>{

		private Text word = new Text();
		private String year,flightDate,origin,destination,ArrDelayMinutes,
		cancelled,diverted,ArrTime,DepTime,airlineID,month;
		private int months,airline;
		@Override
		protected void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			CSVParser parser = new CSVParser();
			String[] lines = parser.parseLine(value.toString());
             //Extracting all the required fields from the Input data
			year=lines[0];
			month=lines[2];
			flightDate=lines[5];
			airlineID=lines[7];
			ArrDelayMinutes=lines[37];
			cancelled=lines[41];
            //Checking Condition for Null Values
			if(!(year.equals(""))&&!(month.equals(""))&&
					!(ArrDelayMinutes.equals("")) &&!(airlineID.equals("")) && !(cancelled.equals("")))
			{
            //Checking Condition for year which is equal to 2008 and the flight should not be cancelled
				if(year.equals("2008") && !(cancelled.equals("1")))	
				{
					months=Integer.parseInt(month);
					airline=Integer.parseInt(airlineID);
					DoubleWritable values = new DoubleWritable(Double.parseDouble(ArrDelayMinutes));
					IntPair x=new IntPair();
					x.set(airline, months);
					context.write(x, values);//Writing out key and value from the Map

				}
			}

		}
	} 

           // FirstPartitioner partions based on the the unique AirlineID
	public static class FirstPartitioner
	extends Partitioner<IntPair, DoubleWritable> {

		@Override
		public int getPartition(IntPair key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.getFirst()) % numPartitions;
		}
	}

        //Overriding the compare method and making own comparator
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntPair.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int comparator;

			if(ip1.getFirst() == ip2.getFirst()){
				comparator =0;
			}else if(ip1.getFirst() > ip2.getFirst()){
				comparator = 1;
			}else{
				comparator = -1;
			}
			if (comparator != 0) {
				return comparator;
			}

			if(ip1.getSecond() == ip2.getSecond()){
				comparator =0;
			}else if(ip1.getSecond() > ip2.getSecond()){
				comparator = 1;
			}else{
				comparator = -1;
			}

			return comparator; //reverse
		}
	}

       //Overriding the compare method and making own comparator
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(IntPair.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int comparator;

			if(ip1.getFirst() == ip2.getFirst() && ip1.getSecond() == ip2.getSecond()){ 
				comparator = 0;
			}else if(ip1.getFirst() > ip2.getFirst() ){
				comparator = 1;
			}else{
				comparator = -1;
			}
			return comparator;
		}
	}

      //Gets the Average arrival delay for each flight per particular month
      //Inputs a key from map which is IntPair and Input value as DoubleWritable and gives out 
      //keys and values as text.
	public static class FlightReducer 
	extends Reducer<IntPair,DoubleWritable,Text,Text> {		
		private ArrayList<int[]> entry = new ArrayList<int[]>();
		private Map<Integer,ArrayList<int[]>> hashed= new LinkedHashMap<Integer,ArrayList<int[]>>();
		int arrDelay;
		@Override
		protected void reduce(IntPair key, Iterable<DoubleWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			double average=0;
			int count=0;
               //Loop to Iterate over all the arrival delay and gets the average after coming outside loop
			for(DoubleWritable delay:values)
			{ count++;
			average+=delay.get();
			}
			arrDelay=(int)Math.ceil(average/count);
			int[] data={key.getSecond(),arrDelay};
			if(!hashed.containsKey(key.getFirst()))
			{
				entry=new ArrayList<>();
				entry.add(data);
				hashed.put(key.getFirst(), entry);
			}
			else
				hashed.get(key.getFirst()).add(data);

		}
          //After getting all the values in LinkedHashMap printing it out using Iterator and in valid format
		public void cleanup(Context context) throws IOException, InterruptedException
		{

			int key;
			String temp="", temp2 = "";
			ArrayList<int[]> arr= null;
			Iterator it = hashed.entrySet().iterator();
			while (it.hasNext()){
				Map.Entry entry = (Map.Entry)it.next();
				key = (int)entry.getKey();
				arr = (ArrayList<int[]>) entry.getValue();
				temp = ""+key+",";
				for (int[] str : arr){
					temp2 += "("+str[0]+","+str[1]+"),";
				}
				temp2 = temp2.substring(0, temp2.length()-1);
				context.write(new Text(temp), new Text(temp2));
				temp2 = "";

			}

		}

	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
         //Setting up Mapper and Reducer output keys and values as the Mapper emits IntPair key and DoubleWritable Value 
         //while the Reducer emits Texts as key and value
         //Also setting up the partitioner,key comparator and group comparator classes. 
		Job job = new Job(conf, "Flight details Job1");
		job.setJarByClass(flightdetails.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(FlightReducer.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
