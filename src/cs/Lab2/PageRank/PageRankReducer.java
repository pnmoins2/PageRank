package cs.Lab2.PageRank;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class PageRankReducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> {
	private int numberOfNodes = 75879;
	private float dampingFactor = new Float("0.85");
	private FloatWritable newVectorCoefficient = new FloatWritable();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		dampingFactor = context.getConfiguration().getFloat("dampingFactor", new Float("0.85"));
		numberOfNodes = context.getConfiguration().getInt("numberOfNodes", 75879);
	}
	
    // Overriding of the reduce function
	// Input : (index, List of subCoefficients)
	// Output : (index, newCoeficient)
    @Override
    protected void reduce(final IntWritable cleI, final Iterable<FloatWritable> listevalI, final Context context) throws IOException,InterruptedException
    {
    	// Initiate the local variables
        Float sum = new Float("0");
        Float currentCoefficient;
        
        // We sum all the sub coefficients
        Iterator<FloatWritable> iterator = listevalI.iterator();
        while (iterator.hasNext())
        {
        	currentCoefficient = iterator.next().get();
        	
        	sum += currentCoefficient;
        }
    	
    	// Output
        sum = dampingFactor * sum + (1 - dampingFactor) / numberOfNodes;
    	newVectorCoefficient.set(sum);
    	context.write(cleI, newVectorCoefficient);
    }
}
