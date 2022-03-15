import java.io.IOException;
import java.util.*; 
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class w1_mapreduce {
    public static class PhoneInfoArrayWrite extends ArrayWritable {
        public PhoneInfoArrayWrite() {
            super(IntWritable.class);
        }

        public PhoneInfoArrayWrite(int[] ints) {
            super(IntWritable.class);
            IntWritable[] intWritable = new IntWritable[ints.length];
            for (int i = 0; i < ints.length; i++) {
                intWritable[i] = new IntWritable(ints[i]);
            }
            set(intWritable);
        }

        public IntWritable[] get() {
            //return (IntWritable[]) super.get();
            Writable[] temp = super.get();
            IntWritable[] values = new IntWritable[temp.length];
            for (int i = 0; i < temp.length; i++) {
                values[i] = (IntWritable)temp[i];
            }
            return values;
        }

        @Override
        public String toString() {
            IntWritable[] values = get();
            String strings = values[0].toString();
            for (int i = 1; i < values.length; i++) {
                strings = strings + "	"+ values[i].toString();
            }
            return strings;
        }
    }
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, ArrayWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] parts = line.split("	");
            int[] arr_int = new int[] {Integer.parseInt(parts[8]),Integer.parseInt(parts[9]),Integer.parseInt(parts[9])+Integer.parseInt(parts[8])};
            output.collect(new Text(parts[1]), new PhoneInfoArrayWrite(arr_int));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, PhoneInfoArrayWrite, Text, PhoneInfoArrayWrite> {
        public void reduce(Text key, Iterator<PhoneInfoArrayWrite> values, OutputCollector<Text, PhoneInfoArrayWrite> output, Reporter reporter) throws IOException {
            PhoneInfoArrayWrite tmp;
            int[] total = new int[3];
            while (values.hasNext()) {
                tmp = values.next();
                IntWritable[] vals = tmp.get();
                for (int i = 0; i < vals.length; i++) {
                    total[i] += vals[i].get();
                }
            }
            output.collect(key, new PhoneInfoArrayWrite(total));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        JobConf job = new JobConf(conf, w1_mapreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneInfoArrayWrite.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setNumMapTasks(5);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJobName("Week1_job_Phone");

        JobClient.runJob(job);
    }
}
