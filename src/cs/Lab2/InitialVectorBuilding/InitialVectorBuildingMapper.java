package cs.Lab2.InitialVectorBuilding;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InitialVectorBuildingMapper extends Mapper<Text, Text, Text, IntWritable> {
	private Text nodeText = new Text("node");
	private IntWritable nodeNumber = new IntWritable();
	
	// Overriding of the map method
	// Input : (firstPart, secondPart)
	// Output : ("node", firstNode), ("node", secondNode)
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
    	{	
		// Recover the key
		String key = keyE.toString();

		// If we are not in the description part
		if (!key.contains("#"))
		{
			// Recover the first node
			nodeNumber.set(Integer.parseInt(key));
			context.write(nodeText, nodeNumber);

			// Recover the second node
			String val = valE.toString();
			nodeNumber.set(Integer.parseInt(val));
			context.write(nodeText, nodeNumber);
		}
    	}
}
