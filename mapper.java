import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		/**
		 * LongWritable, IntWritable, Text ���� Hadoop ��ʵ�ֵ����ڷ�װ Java �������͵��࣬
		 * ��Щ��ʵ����WritableComparable�ӿڣ� ���ܹ������л��Ӷ������ڷֲ�ʽ�����н������ݽ�����
		 * ����Խ����Ƿֱ���Ϊlong,int,String �����Ʒ��
		 */
		// IntWritable one �൱�� java ԭ������ int 1
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// ÿ�м�¼������� map ���������˴���ÿ�ж����ִ�
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				// ���ÿ���ʼ�����ֵĴ��� 1������ <word1,1><word2,1><word1,1>
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			// key ��ͬ�ļ�ֵ�Իᱻ�ַ���ͬһ�� reduce�д���
			// ���� <word1,<1,1>>�� reduce1 �д�����<word2,<1>> ���� reduce2 �д���
			int sum = 0;
			// ��ͬ��key�����ʣ��ĳ��ִ����ᱻ sum �ۼ�
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			// 1�� reduce ������1 ����ֵ�Ժ󣬻������ key�����ʣ���Ӧ�Ľ�������ִ�����
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// �����hadoop��Ⱥ�У�����ʹ�õĶ���
		conf.set("mapred.job.queue.name", "regular");
		// ֮���Դ˴���ֱ���� argv[1] �����ģ���Ϊ���ų�������ʱ�ļ�Ⱥ���Բ�����������в�����
		// �õ��û�����Ĵ���������·����Ϣ��
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (String argsStr : otherArgs) {
			System.out.println("-->> " + argsStr);
		}
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		// map��reduce ���������
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// �������·��
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		// ����job�����У����Ա�֤������job����ִ��
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}