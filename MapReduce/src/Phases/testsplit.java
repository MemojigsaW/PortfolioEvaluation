package Phases;

import DataTypes.Parameters;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class testsplit {
    public static class splitMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Parameters parameters;
        private Text dateText = new Text();
        private Text valText = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String para_serialized = conf.get("parameters");
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
            String[] prcs_selected = new String[selectedindices.size()];

            if (key.get() == 0 && value.toString().contains("date") /*Some condition satisfying it is header*/)
                return;

            try {
                Date date = new SimpleDateFormat("MM/dd/yyyy").parse(date_str);
                if ((date.compareTo(parameters.getMidDate()) >= 0) && (date.compareTo(parameters.getTestDate())<0)){

                    for (int i =0; i<selectedindices.size();i++){
                        int index = selectedindices.get(i);
                        prcs_selected[i] = prcs_str[index];
                    }

                    dateText.set(date_str);
                    valText.set(String.join(",", prcs_selected));

                    context.write(dateText, valText);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }
    public static class splitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val: values){
                context.write(key, val);
            }
        }
    }
}
