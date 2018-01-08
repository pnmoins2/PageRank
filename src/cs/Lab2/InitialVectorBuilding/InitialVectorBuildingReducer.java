package cs.Lab2.InitialVectorBuilding;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InitialVectorBuildingReducer extends Reducer<Text,IntWritable,IntWritable,FloatWritable> {
	private List<Integer> vectorIndexList = new ArrayList<Integer>();
	private IntWritable vectorIndex = new IntWritable();
	private FloatWritable vectorCoefficient = new FloatWritable();
	
    // Overriding of the reduce function
	// Input : ("node", List of nodes)
	// Output : (nodeNumber, 1/numberOfNodes)
    @Override
    protected void reduce(final Text cleI, final Iterable<IntWritable> listevalI, final Context context) throws IOException,InterruptedException
    {
    	// Initiate the local variables
        int currentNode;
        
        // We recover each node one time
        Iterator<IntWritable> firstIterator = listevalI.iterator();
        while (firstIterator.hasNext())
        {
        	currentNode = firstIterator.next().get();
        	
        	if (!vectorIndexList.contains(currentNode))
        	{
        		vectorIndexList.add(currentNode);
        	}
        }
        
        // Compute the initial coefficient
        vectorCoefficient.set((float) 1 / vectorIndexList.size());
        
        // We create the initial vector
        for(int index : vectorIndexList){           
           vectorIndex.set(index);
           
           context.write(vectorIndex, vectorCoefficient);
        }
    }
}
