package cs.Lab2.MatrixBuilding;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MatrixBuildingReducer extends Reducer<IntWritable,IntWritable,Text,FloatWritable> {
	private Text matrixPosition = new Text();
	private FloatWritable matrixCoefficient = new FloatWritable();
	
    	// Overriding of the reduce function
	// Input : (firstNode, List of arrivalNodes)
	// Output : ([firstNode,secondNode], matrixCoefficient)
    	@Override
    	protected void reduce(final IntWritable cleI, final Iterable<IntWritable> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Initiate the local variables
		int currentNode;
		int arrivalNodeCount = 0;
		List<Integer> arrivalNodeList = new ArrayList<Integer>();

		// We count the number of nodes we can reach 
		Iterator<IntWritable> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			// We recover the arrival node and we store it for the next step
			currentNode = iterator.next().get();
			arrivalNodeList.add(currentNode);

			// We add this new node in our count
			arrivalNodeCount += 1;
		}

		// Initiate the local variables for the second step
		String firstNode = String.valueOf(cleI.get());
		String secondNode;
		String indices;

		// Compute the matrix coefficient for the line
		matrixCoefficient.set((float) 1 / arrivalNodeCount); 

		// For each edge (firstNode - secondNode)
		for(int arrivalNode : arrivalNodeList)
		{
			// Compute the matrix position
			secondNode = String.valueOf(arrivalNode);
			indices = firstNode + "," + secondNode;
			matrixPosition.set(indices);

			// Output
			context.write(matrixPosition,matrixCoefficient);
		}
    	}

}
