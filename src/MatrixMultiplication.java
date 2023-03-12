import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

	public static int dim_1 = 64;
	public static int dim_2 = 256;
	public static int dim_3 = 128;

	public static class MatrixMultiplicationMapper extends Mapper < LongWritable, Text, Text, Text >
	{
		Configuration conf = new Configuration();
		private Text map_key = new Text();
		private Text map_value = new Text();

		@Override 
		public void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException { 

			String[] input_values = value.toString().split(" ");

			String id = input_values[0];
			int row_index = Integer.parseInt(input_values[1]);
			int column_index = Integer.parseInt(input_values[2]);
			int i_j = Integer.parseInt(input_values[3]);
			IntWritable num = new IntWritable();
			num.set(i_j);

			if(id.equals("M")){
				for(int k=0; k<dim_3 ; k++){
					map_key.set(row_index+","+k);
					map_value.set(id+","+column_index+","+num);
					context.write(map_key,map_value);
				}	
			}

			else{
				for(int i=0; i<dim_1 ; i++){
					map_key.set(i+","+column_index);
					map_value.set(id+","+row_index+","+num);
					context.write(map_key,map_value);
				}
			}
		} 


		public static class MatrixMultiplicationReducer extends Reducer < Text, Text, Text, Text > 
		{ 
			Text result = new Text(); 
			Configuration conf = new Configuration();
			@Override 
			public void reduce( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException { 

				HashMap<Integer, Integer> Matrix_M = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Matrix_N = new HashMap<Integer, Integer>();

				String[] input_values;
				int j;

				for(Text val:values){
					input_values = val.toString().split(",");
					if(input_values[0].equals("M")){
						Matrix_M.put(Integer.parseInt(input_values[1]), Integer.parseInt(input_values[2]));
					}
					if(input_values[0].equals("N")) {
						Matrix_N.put(Integer.parseInt(input_values[1]), Integer.parseInt(input_values[2]));
					}
				}

				int m_val = 0 , n_val = 0, Matrix_P=0;

				for(j=0 ; j<dim_2 ; j++){
					if(Matrix_M.containsKey(j)){
						m_val = Matrix_M.get(j);
					}
					if(Matrix_N.containsKey(j)){
						n_val = Matrix_N.get(j);
					}
					Matrix_P += m_val * n_val ;

				}

				result.set(Integer.toString(Matrix_P));
				context.write(key,result);
			} 
		}

		public static void main(String[] args) throws Exception
		{ 
			Configuration conf = new Configuration();
			Job job = Job.getInstance( conf, "Matrix Multiplication");
			job.setJarByClass(MatrixMultiplication.class);

			FileInputFormat.addInputPath( job, new Path("input")); 
			FileOutputFormat.setOutputPath( job, new Path("output")); 
			job.setMapperClass( MatrixMultiplicationMapper.class); 
			job.setReducerClass( MatrixMultiplicationReducer.class);
			job.setOutputKeyClass( Text.class); 
			job.setOutputValueClass(Text.class);
			System.exit( job.waitForCompletion(true) ? 0 : 1); 
		} 
	}
}

