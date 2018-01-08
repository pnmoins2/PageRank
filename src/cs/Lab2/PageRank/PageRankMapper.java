package cs.Lab2.PageRank;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class PageRankMapper extends Mapper<Text, Text, IntWritable, FloatWritable> {
	private int numberOfNodes = 75879;
	private float dampingFactor = new Float("0.85");
	private IntWritable index = new IntWritable();
	private FloatWritable coefficient = new FloatWritable();
	private Map<Integer, Float> vector = new HashMap<Integer,Float>();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		dampingFactor = context.getConfiguration().getFloat("dampingFactor", new Float("0.85"));
		numberOfNodes = context.getConfiguration().getInt("numberOfNodes", 75879);
		
		URI[] cachedFiles = context.getCacheFiles();
		
		if (cachedFiles != null && cachedFiles.length > 0)
		{
			Path vectorPath = new Path(cachedFiles[0].toString());
			FileSystem fs = FileSystem.newInstance(context.getConfiguration());
			FSDataInputStream in = fs.open(vectorPath);
			
			try{
				InputStreamReader isr = new InputStreamReader(in);
				BufferedReader br = new BufferedReader(isr);
				
				// read line by line
				String line = br.readLine();
				
				while (line !=null){
					// Split the information in a line
					StringTokenizer tokenizer = new StringTokenizer(line);
					
					int key = Integer.parseInt(tokenizer.nextToken());
					Float value = Float.parseFloat(tokenizer.nextToken());
					
					// We fill the vector
					vector.put(key, value);
					
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
	
	// Overriding of the map method
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
    {	
		// Recover the index
        String inputIndex = keyE.toString();
        
        // Split the index
        String[] inputIndexSplit = inputIndex.split(",");
        
        // Recover the column index
	    index.set(Integer.parseInt(inputIndexSplit[1]));
	    
        // Recover the matrix coefficient
	    Float matrixCoefficient = Float.parseFloat(valE.toString());
	    
	    // Recover the vector coefficient linked to the matrix coefficient, 
	    // if it's not defined, then we didn't compute it in the previous iteration
	    // then all the matrix coefficient in the corresponding column are 0
	    // So the vector coefficient should be (1 - dampingFactor) / numberOfNodes
	    if (vector.containsKey(Integer.parseInt(inputIndexSplit[0])))
        {
	    	coefficient.set(matrixCoefficient * vector.get(Integer.parseInt(inputIndexSplit[0])));	
        }
	    else
	    {
	    	coefficient.set(matrixCoefficient * (1 - dampingFactor)/numberOfNodes);
	    }
	    
	    // Output
	    context.write(index, coefficient);
    }
}