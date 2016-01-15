package com.cloudera.spark.graph.kshell;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Random;

/**
 * Created by root on 4/27/15.
 */
public class CdrRandomGenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public CdrRandomGenMapper() {}

    @Override
    public void setup(Context context) {

    }

    @Override
    public void map(LongWritable offset, Text value, Context context)
            throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ',');
        int id = Integer.valueOf(split[0]);
        int k = Integer.valueOf(split[1]);
        int total = Integer.valueOf(split[2]);
        int itrEachRound = Integer.valueOf(split[3]);
        NullWritable key = NullWritable.get();
        Text pair = new Text();
        Random rand = new Random();

        for (int l = 0; l < itrEachRound; l++) {
            for (int j = 0; j < k; j++) {
                pair.set(id + "," + rand.nextInt(total));
                context.write(key, pair);
            }
        }
    }
}
