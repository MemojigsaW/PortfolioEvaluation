package Phases;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Phase2 {
    public static class meanMapper extends Mapper<Text, Text, IntWritable, DoubleWritable>{

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable int_key = new IntWritable();
            DoubleWritable dle_val = new DoubleWritable();

            String val_str = value.toString();
            String[] parse_str = val_str.split(",");

            for (int i=0; i<parse_str.length;i++){
                int_key.set(i);
                dle_val.set(Double.parseDouble(parse_str[i]));
                context.write(int_key, dle_val);
            }
        }
    }

    public static class meanReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double mean = 0;
            int count = 0;
            for (DoubleWritable val: values){
                mean+=val.get();
                count+=1;
            }
            context.write(key, new DoubleWritable(mean/count));
        }
    }
}
