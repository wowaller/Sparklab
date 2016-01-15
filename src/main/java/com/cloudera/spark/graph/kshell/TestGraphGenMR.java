package com.cloudera.spark.graph.kshell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

/**
 * Created by root on 4/27/15.
 */
public class TestGraphGenMR {

    public static void main(String[] args) {

        String output = "";
        int v = 1;
        int partition = 1;
        int k = 1;
        int itrEachRound = 1;

        if (args.length != 5) {
            printUsage();
            System.exit(0);
        } else if (args.length == 5) {
            output = args[0];
            v = Integer.valueOf(args[1]);
            partition = Integer.valueOf(args[2]);
            k = Integer.valueOf(args[3]);
            itrEachRound = Integer.valueOf(args[4]);
        }

        Configuration conf = new Configuration();
        String tmpDir = conf.get("hadoop.tmp.dir");

        try {
            FileSystem fs = FileSystem.get(conf);
            Path tmpPath = new Path(tmpDir + "/cdrGen");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(tmpPath, true)));
            Random rand = new Random();

            for (int i = 0; i < v; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(i);
                sb.append(",");
                sb.append(rand.nextInt(k));
                sb.append(",");
                sb.append(v);
                sb.append(",");
                sb.append(itrEachRound);
                sb.append("\n");
                writer.write(sb.toString());
            }

            writer.flush();
            writer.close();

            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(TestGraphGenMR.class);
            job.setMapperClass(CdrRandomGenMapper.class);
            job.setReducerClass(Reducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(NLineInputFormat.class);
            job.setNumReduceTasks(0);

            FileInputFormat.addInputPath(job, tmpPath);
            FileOutputFormat.setOutputPath(job, new Path(output));

            NLineInputFormat.setNumLinesPerSplit(job, v / partition);

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void printUsage() {
        System.out.println("Usage: cmd <output> <vertex> <partition> <k> <itrPerRound>");
    }

}
