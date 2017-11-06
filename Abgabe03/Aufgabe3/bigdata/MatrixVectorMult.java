package bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatrixVectorMult {

    public static void printUsageAndQuit() {
        System.out.println("Please specifiy the two parameters <Pfad Matrix Eingabedatei> <Pfad Vector Eingabedatei> <Ausgabepfad>");
        System.exit(0);
    }
	
	

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            printUsageAndQuit();
        Configuration conf = new Configuration();

		DoubleListWritable vector = new DoubleListWritable();
		
		BufferedReader br = null; //read vector manually and save under configuration
		FileReader fr = null;

		try {

			fr = new FileReader(args[1]);
			br = new BufferedReader(fr);

			conf.set("vector",br.readLine());
			
			} catch (IOException e) {

				e.printStackTrace();

			} finally {

				try {

					if (br != null)
						br.close();

					if (fr != null)
						fr.close();

				} catch (IOException ex) {

					ex.printStackTrace();

				}

			}




        Job job = Job.getInstance(conf, "MatrixVectorMult");
        job.setJarByClass(MatrixVectorMult.class);
        job.setMapperClass(MVMultMapper.class);
        job.setReducerClass(MVMultReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleListWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
