import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Comparator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    private Job job;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "PopularityLeague");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(PopularityLeagueMap.class);
        job.setReducerClass(PopularityLeagueReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(PopularityLeague.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }
    public static class PopularityLeagueMap extends Mapper<Object, Text, IntWritable, IntWritable> {

        String[] leaguesPath;
        List<Integer> leaguesInput;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            leaguesPath = conf.getStrings("league");
            String output = readHDFSFile(leaguesPath[0], conf);
            String[] splitedPages = output.split("\n");
            leaguesInput = new ArrayList<Integer>();
            for(String s : splitedPages){
                leaguesInput.add(Integer.valueOf(s));
            }
        }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken().trim();
                if (!nextToken.contains(":")) {
                    if(leaguesInput.contains(Integer.valueOf(nextToken))){
                        context.write(new IntWritable(Integer.valueOf(nextToken)), new IntWritable(1));
                    }
                }
            }
        }
    }


    public static class PopularityLeagueReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        Map<Integer, Integer> buffer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            buffer = new HashMap<Integer, Integer>();
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            buffer.put(key.get(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            Set<Map.Entry<Integer, Integer>> set = buffer.entrySet();
            List<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>(set);

            Collections.sort( list, new Comparator<Map.Entry<Integer, Integer>>()
            {
                public int compare( Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2 )
                {
                    return (o1.getValue()).compareTo( o2.getValue() );
                }
            } );

            Integer lastRank =0;
            Integer opset=1;
            for(int i= 0; i<list.size(); i++){
                if(i==list.size()-1 || list.get(i).getValue().equals(list.get(i+1).getValue())){
                    context.write(new IntWritable(list.get(i).getKey()), new IntWritable(lastRank));
                    opset++;
                }
                else{
                    context.write(new IntWritable(list.get(i).getKey()), new IntWritable(lastRank));
                    lastRank+=opset;
                    opset=1;

                }
            }
        }
    }
}

