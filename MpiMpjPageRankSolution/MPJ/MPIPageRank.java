

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import mpi.*;
public class MPIPageRank
{
	public static int mpi_pagerank(HashMap<Integer,ArrayList<Integer>>adjacencyMatrix,ArrayList<Integer>amIndex,
			int numUrls, int totalNumUrls, int numIterations, double threshold,
			double[] rank_values_table, Intracomm communicator)
	{
           
            /* Definitions of variables */
            double[] delta=new double[1];
            delta[0]=0.0;
            Double damping_factor=0.85;
            int loop =0;
            ArrayList<Integer> target_urls_list;
            double[] local_rank_values_table = new double[totalNumUrls];
            double[] old_rank_values_table = new double[totalNumUrls];
            int source_url,target_url, rank;
            Double intermediate_rank_value, danglingValue=0.0,sumDangling=0.0;
            rank = communicator.Rank();

            //Initialized page rank
            initializeRankValueTable(rank_values_table,totalNumUrls);

		/* Start computation loop */
            /* to Compute intermediat pageranks and dangling values */
            do
            {
                //initialize the local_rank_values_table and dangling value
                    for(int i=0; i<totalNumUrls;i++)
                    {
                        local_rank_values_table[i]=0.0;
                        old_rank_values_table[i]=rank_values_table[i];
                    }
                    danglingValue=0.0;
                //traverse the local partition
                    for(int src=0;src<amIndex.size();)
                    {
                       source_url = amIndex.get(src);
                       src=src+2;
                       target_urls_list = adjacencyMatrix.get(source_url);
                       int outdegree_of_source_url = target_urls_list.size();
                       for (int i=0; i<outdegree_of_source_url;i++)
                       {
                            target_url = target_urls_list.get(i).intValue();
                            intermediate_rank_value = local_rank_values_table[target_url]+
                                                    rank_values_table[source_url]/(double)outdegree_of_source_url;
                            local_rank_values_table[target_url]= intermediate_rank_value;
                                
                        }//for

                        if (outdegree_of_source_url == 0)
                        {
                            danglingValue += rank_values_table[source_url];
                        }//IF
                      }//for
         
                    /* combine intermediate pagerank values */
                    communicator.Allreduce(local_rank_values_table,0, rank_values_table,0,totalNumUrls, MPI.DOUBLE, MPI.SUM);
           
                    /* combine dangling values */
                    double[] danglingArr=new double[1];
                    double[] sumDanglingArr=new double[1];
                    danglingArr[0]=danglingValue;
                    communicator.Allreduce(danglingArr,0, sumDanglingArr,0, 1, MPI.DOUBLE, MPI.SUM);
                    sumDangling = sumDanglingArr[0];
		/*  root process recalculates the page rank values with damping factor 0.85 */
		if(rank ==0)
                {
                    double dangling_value_per_page = sumDangling / (double)totalNumUrls;
                    for (int i=0;i<totalNumUrls;i++)
                    {
                            rank_values_table[i]=rank_values_table[i]+dangling_value_per_page;
                    }
                    for (int i=0;i<totalNumUrls;i++)
                    {
                            rank_values_table[i]= damping_factor*rank_values_table[i]+(1-damping_factor)*(1.0/(double)totalNumUrls);
                    }
                   /* Root(process 0) computes delta to determine to stop or continue */
                       
                    /* Root broadcasts delta */
                        /*  Calculation of  delta */
                    delta[0] = 0.0;
                    double diff = 0.0;
                    for (int i=0;i<totalNumUrls;i++)
                    {
                        diff = old_rank_values_table[i] - rank_values_table[i];
                        delta[0] += diff*diff;
                        old_rank_values_table[i]=rank_values_table[i];
                    }
            }//if rank ==0
               communicator.Bcast(delta, 0,1, MPI.DOUBLE, 0);
               communicator.Bcast(rank_values_table, 0,totalNumUrls, MPI.DOUBLE, 0);
                      
	}while (delta[0] > threshold && ++loop < numIterations);
       
		return 1;
	}
 public static void initializeRankValueTable(double[] paramRankValuesTable, int paramAdjacencyMatrixSize)
  {

	  //System.out.println ("Rank Values table initialized");
   double d = 1.0D / paramAdjacencyMatrixSize;
    for (int i = 0; i < paramAdjacencyMatrixSize; i++)
    {
    	paramRankValuesTable[Integer.valueOf(i)]= Double.valueOf(d);
    }// for ends


  }
}