package cs.Lab2.Sort;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Text, Text, FloatWritable, Text> {
	private FloatWritable minusPageRank = new FloatWritable();
	
	// Overriding of the map method
	// Input : (node, pageRank)
	// Output : (-pageRank, node)
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
    	{		
		// Recover the pageRank
		float pageRank = Float.parseFloat(valE.toString());
		
		minusPageRank.set(-pageRank);
		context.write(minusPageRank, keyE);
    	}
}
