package cs.Lab2.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;


public class PageRank extends Configured implements Tool {
	private Configuration conf;
	
	public PageRank(Configuration conf)
	{
		this.conf = conf;
	}
	
    	public int run(String[] args) throws Exception {
		if (args.length != 2) {

		    System.out.println("Usage: [input] [output]");

		    System.exit(-1);

		}

		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		Job job = Job.getInstance(conf);

		job.setJobName("PageRank");

		// On précise le format des fichiers d'entrée et de sortie

		job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

		// On précise les classes MyProgram, Map et Reduce

		job.setJarByClass(PageRank.class);

		job.setMapperClass(PageRankMapper.class);

		job.setReducerClass(PageRankReducer.class);


		// Définition des types clé/valeur de notre problème

		job.setMapOutputKeyClass(IntWritable.class);

		job.setMapOutputValueClass(FloatWritable.class);


		job.setOutputKeyClass(IntWritable.class);

		job.setOutputValueClass(FloatWritable.class);

		// Récupération du numéro de l'itération courante et de l'itération précédente
		int currentIteration = job.getConfiguration().getInt("currentIteration", 1);
		int previousIteration = currentIteration - 1;

		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

		Path inputMatrixPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputMatrixPath);

			Path inputVectorPath = new Path(args[1] + String.valueOf(previousIteration) + "/part-r-00000");
			job.addCacheFile(inputVectorPath.toUri());


		Path outputFilePath = new Path(args[1] + String.valueOf(currentIteration));
		FileOutputFormat.setOutputPath(job, outputFilePath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(job.getConfiguration());

		if (fs.exists(outputFilePath)) {
		    fs.delete(outputFilePath, true);
		}


		return job.waitForCompletion(true) ? 0: 1;

    	}


    	public static void main(String[] args) throws Exception {
    		Configuration config = new Configuration();
    	
		config.setInt("numberOfNodes", 75879);
		config.setInt("maxNodeNumber", 75887);
		config.setFloat("dampingFactor", new Float("0.85"));
		
		for (int i = 1; i <= 5; i++)
		{
			config.setInt("currentIteration", i);
			
			PageRank pageRankDriver = new PageRank(config);
			
			ToolRunner.run(pageRankDriver, args);
		}
    	}

}
