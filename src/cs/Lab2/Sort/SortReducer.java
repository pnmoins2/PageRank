package cs.Lab2.Sort;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SortReducer extends Reducer<FloatWritable,Text,Text,FloatWritable> {
	private Text node = new Text();
	private FloatWritable pageRank = new FloatWritable();
	
    	// Overriding of the reduce function
	// Input : (-pageRank, List of Nodes)
	// Output : (node, pageRank)
	@Override
    	protected void reduce(final FloatWritable cleI, final Iterable<Text> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Recover the pageRank
		pageRank.set(-cleI.get());

		// For each node
		Iterator<Text> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			// Recover the node
			node.set(iterator.next().toString());

			// Output
			context.write(node, pageRank);
		}
    	}
}
