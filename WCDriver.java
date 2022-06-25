package data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.创建配置文件
        Configuration configuration=new Configuration();
        Job job =Job.getInstance(configuration,"wordcount");

        //2.设置jar的位置
        job.setJarByClass(WCDriver.class);

        //3.设置Map和Reduce的位置
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setCombinerClass(WCCombiner.class);

        //4.设置map输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置reduce的输出key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0])); //设置输入的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1])); // 设置输出路径

        //7.提交程序运行
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }


}

