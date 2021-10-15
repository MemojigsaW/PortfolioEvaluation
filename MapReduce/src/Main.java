import DataTypes.Parameters;
import Phases.Phase1;
import Phases.Phase2;
import Phases.Phase3;
import Phases.testsplit;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/*
args[0] is config file location
args[1:] are Tickers
 */
public class Main {
    private static JSONObject config = null;
    private static ArrayList<Integer> selectIndex = new ArrayList<>();

    public static void readConf(String path){
        JSONParser parser = new JSONParser();
        try{
            Object obj = parser.parse(new FileReader(path));
            config = (JSONObject) obj;
            System.out.println("Config Found");
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static boolean checkTickers(String[] tickers){
        Set<String> lump = new HashSet<String>();
        for (String i : tickers)
        {
            if (lump.contains(i)) {
                return false;
            }
            lump.add(i);
        }

        JSONArray jArray = (JSONArray)config.get("allowedTickers");
        List<String> alltickers = new ArrayList<String>();
        alltickers.addAll(jArray);

        if (!alltickers.containsAll(Arrays.asList(tickers))){
            return false;
        }

        for (String one_ticker:tickers){
            selectIndex.add(alltickers.indexOf(one_ticker));
        }

        return true;
    }

    public static void purgeDir(File dir){
        for (File file: dir.listFiles()){
            if (file.isDirectory()){
                purgeDir(file);
            }
            file.delete();
        }
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InterruptedException {
//        clean args
        readConf(args[0]);
        String[] tickers = Arrays.copyOfRange(args, 4, args.length);
        if (!checkTickers(tickers)){
            System.out.println("Either not in available or there is duplicate");
            System.exit(1);
        }
//        clean local file system
        File dir_root = new File((String) config.get("file_Loc"));
        purgeDir(dir_root);

//        serialize parameters to pass to mapper/reducers
        Parameters para = new Parameters();
        para.setSelectedIndices(selectIndex);
        para.setStartDate(new SimpleDateFormat("MM/dd/yyyy").parse(args[1]));
        para.setMidDate(new SimpleDateFormat("MM/dd/yyyy").parse(args[2]));
        para.setTestDate(new SimpleDateFormat("MM/dd/yyyy").parse(args[3]));

        Gson gson = new Gson();
        String para_serialized = gson.toJson(para);

//        split task, not in parallel, need least 2 node cluster, see scheduler documnetation
        Configuration conf_s = new Configuration();
        conf_s.set("parameters", para_serialized);
        conf_s.set("mapreduce.output.textoutputformat.separator", ",");

        Job js = Job.getInstance(conf_s, "split");
        js.setJarByClass(Main.class);
        js.setMapperClass(testsplit.splitMapper.class);
        js.setReducerClass(testsplit.splitReducer.class);
        js.setMapOutputKeyClass(Text.class);
        js.setMapOutputValueClass(Text.class);
        js.setOutputKeyClass(Text.class);
        js.setOutputValueClass(Text.class);
        js.setInputFormatClass(TextInputFormat.class);
        js.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(conf_s);
        Path output_js = new Path((String)config.get("hdfs_output")+"test");
        Path input_path = new Path((String)config.get("hdfs_input"));

        if (hdfs.exists(output_js)){
            hdfs.delete(output_js, true);
        }

        FileInputFormat.setInputPaths(js, input_path);
        FileOutputFormat.setOutputPath(js, output_js);

        js.waitForCompletion(true);

        hdfs.copyToLocalFile(false, output_js, new Path((String) config.get("file_Loc")), true);
        hdfs.close();

//        configure conf
        Configuration conf = new Configuration();
        conf.set("parameters", para_serialized);

//        Phase 1
        Job j1 = Job.getInstance(conf, "phase1");
        j1.setJarByClass(Main.class);
        j1.setMapperClass(Phase1.retMapper.class);
        j1.setReducerClass(Phase1.retReducer.class);
        j1.setMapOutputKeyClass(IntWritable.class);
        j1.setMapOutputValueClass(Phase1.DaySelectedPrcWritable.class);
        j1.setOutputKeyClass(IntWritable.class);
        j1.setOutputValueClass(Text.class);
        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(TextOutputFormat.class);

        hdfs = FileSystem.get(conf);
        Path output_j1 = new Path((String)config.get("hdfs_output")+"p1");

        if (hdfs.exists(output_j1)){
            hdfs.delete(output_j1, true);
        }

        FileInputFormat.setInputPaths(j1, input_path);
        FileOutputFormat.setOutputPath(j1, output_j1);

        hdfs.close();
        j1.waitForCompletion(true);

//        Phase 2
        Configuration conf2 = new Configuration();
        Job j2 = Job.getInstance(conf2, "phase2");

        j2.setJarByClass(Main.class);
        j2.setMapperClass(Phase2.meanMapper.class);
        j2.setReducerClass(Phase2.meanReducer.class);
        j2.setMapOutputKeyClass(IntWritable.class);
        j2.setMapOutputValueClass(DoubleWritable.class);
        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(DoubleWritable.class);
        j2.setInputFormatClass(KeyValueTextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);

        hdfs = FileSystem.get(conf2);
        Path output_j2 = new Path((String)config.get("hdfs_output")+"p2");
        if (hdfs.exists(output_j2)){
            hdfs.delete(output_j2, true);
        }

        FileInputFormat.setInputPaths(j2, output_j1);
        FileOutputFormat.setOutputPath(j2, output_j2);

        j2.waitForCompletion(true);
        hdfs.copyToLocalFile(false, output_j2, new Path((String) config.get("file_Loc")), true);
        hdfs.close();


        Configuration conf3 = new Configuration();
        conf3.set("parameters", para_serialized);

        Job j3 = Job.getInstance(conf3, "phase3");

        j3.addCacheFile(new Path(String.valueOf(output_j2)+ "/part-r-00000").toUri());

        j3.setJarByClass(Main.class);
        j3.setMapperClass(Phase3.covMapper.class);
        j3.setReducerClass(Phase3.covReducer.class);
        j3.setMapOutputKeyClass(Phase3.IndexKeyWritable.class);
        j3.setMapOutputValueClass(DoubleWritable.class);
        j3.setOutputKeyClass(Phase3.IndexKeyWritable.class);
        j3.setOutputValueClass(DoubleWritable.class);
        j3.setInputFormatClass(KeyValueTextInputFormat.class);
        j3.setOutputFormatClass(TextOutputFormat.class);

        hdfs = FileSystem.get(conf3);
        Path output_j3 = new Path((String)config.get("hdfs_output"));
        if (hdfs.exists(output_j3)){
            hdfs.delete(output_j3, true);
        }

        FileInputFormat.setInputPaths(j3, output_j1);
        FileOutputFormat.setOutputPath(j3, output_j3);

        j3.waitForCompletion(true);

        hdfs.copyToLocalFile(false, output_j3, new Path((String) config.get("file_Loc")), true);

        hdfs.close();
        System.exit(0);
    }
}

//    hadoop jar "C:\Users\Alan\Desktop\Projects\PortfolioEval\MapReduce\out\artifacts\MapReduce_jar\MapREduce.jar" "C:\Users\Alan\Desktop\Projects\PortfolioEval\config.json" 01/01/2011 01/01/2013 01/01/2014 A AAPL ABC ABT ADBE ADI ADM ADP ADSK AEE




