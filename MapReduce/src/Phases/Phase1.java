package Phases;
import DataTypes.Parameters;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
Choose a subset of data
Map prc by month
Calc monthly pct_change
 */
public class Phase1 {
    public static class DaySelectedPrcWritable implements Writable{
        private IntWritable day;
        private ArrayWritable prcs;

//        default constructor
        public DaySelectedPrcWritable(){
            day = new IntWritable();
            prcs = new ArrayWritable(DoubleWritable.class);
        }

//        custom construtor
        public DaySelectedPrcWritable(IntWritable day_in, ArrayWritable prcs_in){
            day = day_in;
            prcs = prcs_in;
        }

        public int compare(DaySelectedPrcWritable input){
            return day.compareTo(input.day);
        }

        public void setDay(IntWritable day_in){
            day = day_in;
        }

        public IntWritable getDay(){
            return day;
        }

        public ArrayWritable getPrcs(){
            return prcs;
        }

        public void setPrcs(ArrayWritable arr){
            prcs = arr;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            day.write(dataOutput);
            prcs.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            day.readFields(dataInput);
            prcs.readFields(dataInput);
        }
    }

    public static class retMapper extends Mapper<LongWritable, Text, IntWritable, DaySelectedPrcWritable>{
        private Configuration conf;
        private String para_serialized;
        private Parameters parameters;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            para_serialized = conf.get("parameters");
            Gson gson = new Gson();
            parameters = gson.fromJson(para_serialized, Parameters.class);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str_val = value.toString();
            String[] parse_str = str_val.split(",");
            String date_str = parse_str[0];
            String[] prcs_str = Arrays.copyOfRange(parse_str, 1, parse_str.length);
            ArrayList<Integer> selectedindices = parameters.getSelectedIndices();

            if (key.get() == 0 && value.toString().contains("date") /*Some condition satisfying it is header*/)
                return;

            try {
                Date date = new SimpleDateFormat("MM/dd/yyyy").parse(date_str);
                if ((date.compareTo(parameters.getStartDate()) >= 0) && (date.compareTo(parameters.getMidDate())<0)){
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    IntWritable day = new IntWritable(cal.get(Calendar.DAY_OF_MONTH));

                    DoubleWritable[] prcs_dle = new DoubleWritable[selectedindices.size()];
                    ArrayWritable prcs_aw = new ArrayWritable(DoubleWritable.class);

                    for (int i =0; i<selectedindices.size();i++){
                        int index = selectedindices.get(i);
                        double dle_val = Double.parseDouble(prcs_str[index]);
                        prcs_dle[i] = new DoubleWritable(dle_val);
                    }
                    prcs_aw.set(prcs_dle);

//                    create key as month and year
                    int year = cal.get(Calendar.YEAR);
                    int month = cal.get(Calendar.MONTH)+1;
                    int k = Integer.parseInt(String.valueOf(year)+String.format("%02d", month));

                    context.write(new IntWritable(k), new DaySelectedPrcWritable(day, prcs_aw));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class retReducer extends Reducer<IntWritable, DaySelectedPrcWritable, IntWritable, Text>{


        private Configuration conf;
        private String para_serialized;
        private Parameters parameters;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            para_serialized = conf.get("parameters");
            Gson gson = new Gson();
            parameters = gson.fromJson(para_serialized, Parameters.class);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<DaySelectedPrcWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable max_int, min_int;
            ArrayWritable max_arr, min_arr;

            max_int = new IntWritable(15);
            min_int = new IntWritable(15);
            max_arr = new ArrayWritable(DoubleWritable.class);
            min_arr = new ArrayWritable(DoubleWritable.class);

            for (DaySelectedPrcWritable val: values){
                if (val.getDay().compareTo(min_int)<0){
                    min_int.set(val.getDay().get());
                    min_arr.set(val.getPrcs().get());
                }else if (val.getDay().compareTo(max_int)>0){
                    max_int.set(val.getDay().get());
                    max_arr.set(val.getPrcs().get());
                }
            }

            int total_size = parameters.getSelectedIndices().size();
            String[] ret_str = new String[total_size];
            Writable[] arr_max = max_arr.get();
            Writable[] arr_min = min_arr.get();

            for (int i =0; i< total_size; i++){
                double end_v = ((DoubleWritable)arr_max[i]).get();
                double start_v = ((DoubleWritable)arr_min[i]).get();
                double ret_d = (end_v - start_v)/start_v;
                String column_ret_str = String.format("%f",ret_d);
                ret_str[i] = column_ret_str;
            }

            context.write(
                    key,
                    new Text(String.join(",", ret_str))
            );

        }
    }
}
