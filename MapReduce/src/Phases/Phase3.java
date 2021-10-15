package Phases;

import DataTypes.Parameters;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;

public class Phase3 {
    public static class IndexKeyWritable implements WritableComparable<IndexKeyWritable>{
        private IntWritable rowIndex;
        private IntWritable columnIndex;

        public IndexKeyWritable(){
            rowIndex = new IntWritable();
            columnIndex = new IntWritable();
        }

        public IndexKeyWritable(IntWritable _row, IntWritable _col){
            rowIndex = _row;
            columnIndex = _col;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            rowIndex.write(dataOutput);
            columnIndex.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            rowIndex.readFields(dataInput);
            columnIndex.readFields(dataInput);
        }

        @Override
        public int compareTo(IndexKeyWritable o) {
            IndexKeyWritable _o = (IndexKeyWritable)o;
            if (rowIndex.compareTo(_o.rowIndex)!=0){
                return rowIndex.compareTo(_o.rowIndex);
            }else{
                return columnIndex.compareTo(_o.columnIndex);
            }
        }

        @Override
        public String toString() {
            return rowIndex + "," + columnIndex;
        }
    }

    public static class covMapper extends Mapper<Text, Text, IndexKeyWritable, DoubleWritable>{
        private Parameters parameters;
        private double[] mean_vals;
        FSDataInputStream in = null;

        private IntWritable r = new IntWritable();
        private IntWritable c = new IntWritable();
        private DoubleWritable dle_val = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String para_serialized = conf.get("parameters");
            Gson gson = new Gson();
            parameters = gson.fromJson(para_serialized, Parameters.class);

            mean_vals = new double[parameters.getSelectedIndices().size()];

            URI[] cachedFiles = context.getCacheFiles();
            if (cachedFiles == null || cachedFiles.length==0 ){
                throw new IOException("Cached file problem");
            }

            FileSystem fs = FileSystem.get(context.getConfiguration());
            in = fs.open(new Path(String.valueOf(cachedFiles[0])));

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
            String line_text;
            while ((line_text = bufferedReader.readLine())!=null){
                String[] parse_str = line_text.split("\t");
                mean_vals[Integer.parseInt(parse_str[0])] = Double.parseDouble(parse_str[1]);
            }
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String[] parse_str = value.toString().split(",");
            for (int i=0; i<parse_str.length;i++){
                for (int j=i;j<parse_str.length;j++){
                    double X = Double.parseDouble(parse_str[i]);
                    double Y = Double.parseDouble(parse_str[j]);
                    double result = (X - mean_vals[i]) * (Y - mean_vals[j]);

                    r.set(i);
                    c.set(j);
                    dle_val.set(result);
                    context.write(new IndexKeyWritable(r,c), dle_val);
                }
            }
        }
    }

    public static class covReducer extends Reducer<IndexKeyWritable, DoubleWritable, IndexKeyWritable, DoubleWritable>{
        private DoubleWritable dle_val = new DoubleWritable();

        @Override
        protected void reduce(IndexKeyWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum=0;
            int count = 0;
            for (DoubleWritable val: values){
                sum+=val.get();
                count+=1;
            }
            dle_val.set(sum/(count-1));
            context.write(key, dle_val);
        }
    }
}
