package cs.Lab2.MatrixBuilding;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixBuildingMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
	private IntWritable firstNode = new IntWritable();
	private IntWritable secondNode = new IntWritable();
	
	// Overriding of the map method
	// Input : (firstPart, secondPart)
	// Output : (firstNode, secondNode) -- Remove the first lines
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
    	{	
		// Recover the first part
		String key = keyE.toString();

		// If we are not in the description part
		if (!key.contains("#"))
		{
			// Recover the first node
			firstNode.set(Integer.parseInt(key));

			// Recover the second node
			String val = valE.toString();
			secondNode.set(Integer.parseInt(val));

			// Output
			context.write(firstNode, secondNode);
		}
    	}
}
