package cs.Lab2;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cs.Lab2.InitialVectorBuilding.InitialVectorBuilding;
import cs.Lab2.MatrixBuilding.MatrixBuilding;
import cs.Lab2.PageRank.PageRank;
import cs.Lab2.Sort.Sort;

public class Conductor {
	private static String initialVectorBuildingOutput = "/2_Iteration0";
	private static String matrixBuildingOutput = "/1_MatrixBuilding";
	private static String iterationsOutput = "/2_Iteration";
	private static String sortOutput = "/3_Sort";
	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {

           		System.out.println("Usage: [input] [output] [numberOfIterations] [dampingFactor]");

            		System.exit(-1);

        	}
		
		// Recover the dampingFactor
		conf.setFloat("dampingFactor", Float.parseFloat(args[3]));
		
		String globalOutput = args[1];
		
		// First Step : initial vector
		InitialVectorBuilding initialVectorBuildingDriver = new InitialVectorBuilding(conf);
		String[] initialVectorBuildingArgs = new String[2];
		initialVectorBuildingArgs[0] = args[0];
		initialVectorBuildingArgs[1] = globalOutput + initialVectorBuildingOutput;
		initialVectorBuildingDriver.run(initialVectorBuildingArgs);
		
		// Intermediate step : Recover the number of nodes within the configuration
		// Recover the vector Location
		String vectorFile = initialVectorBuildingArgs[1] + "/part-r-00000";
		Path vectorPath = new Path(vectorFile);
		
		// Open the file
		FileSystem fs = FileSystem.newInstance(conf);
		FSDataInputStream in = fs.open(vectorPath);
		
		try{
			// Node Counter
			int numberOfNodes = 0;
			
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){				
				// Count the node
				numberOfNodes += 1;
				
				// go to the next line
				line = br.readLine();
			}
			
			conf.setInt("numberOfNodes", numberOfNodes);
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
		
		// Second Step : matrix Building
		MatrixBuilding matrixBuildingDriver = new MatrixBuilding(conf);
		String[] matrixBuildingArgs = new String[2];
		matrixBuildingArgs[0] = args[0];
		matrixBuildingArgs[1] = globalOutput + matrixBuildingOutput;
		matrixBuildingDriver.run(matrixBuildingArgs);
		
		// Third Step : pageRank
		for (int i = 1; i <= Integer.parseInt(args[2]); i++)
		{
			conf.setInt("currentIteration",i);
			PageRank pageRankDriver = new PageRank(conf);
			String[] pageRankArgs = new String[2];
			pageRankArgs[0] = matrixBuildingArgs[1];
			pageRankArgs[1] = globalOutput + iterationsOutput;
			pageRankDriver.run(pageRankArgs);
		}
			
		// Fourth Step : Sort pageRank
		Sort sortDriver = new Sort(conf);
		String[] sortArgs = new String[2];
		sortArgs[0] = globalOutput + iterationsOutput + args[2];
		sortArgs[1] = globalOutput + sortOutput;
		sortDriver.run(sortArgs);
		
		// Fifth Step : Display the first 20 values
		// Recover the Output  Location
		String outputFile = sortArgs[1] + "/part-r-00000";
		Path outputPath = new Path(outputFile);
		
		// Open the file
		fs = FileSystem.newInstance(conf);
		in = fs.open(outputPath);
		
		try{			
			// Line Counter
			int numberLines = 0;
			
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			System.out.println("---- 10 users with the highest pageRank scores ----");
			
			while (numberLines < 10 && line !=null){				
				// Print Line
				System.out.println(line);
				
				// Count the line
				numberLines += 1;
				
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
	}

}
