  

import mpi.* ;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedHashMap;

class MPIMain
{

    static public void main(String[] InputArray) throws MPIException
 {
      
      if (InputArray.length != 7)
           {
             String displayMsg = "\nUsage: java ParallelPageRank [input file path][output file path][iteration count]" +
                     "[Threshold value]" +
                     "\ne.g.: mpjrun.sh -np 3 MPIMain /home/sumayah/Desktop/B534/Project1Part2/pagerank.input.1000.0" +
                     " /home/sumayah/Desktop/B534/Project1Part2/pagerank.output.1000.0 20 0.001\n";
             System.out.println(displayMsg);
             System.exit(-1);
            } 


        //Input FileName
        String inputFileName  = InputArray[3];
        //Output FileName
        String outputFileName = InputArray[4];
        //Number of Iterations
        Integer numIterations= Integer.parseInt(InputArray[5]);
        //ThreshHold
        Double threshold=Double.parseDouble(InputArray[6]);
        //Check if iterationCount is less than 0
        if(numIterations<=0)
            {
            	 String displayMsg = "Iteration Count must be greater than 0";
                 System.out.println(displayMsg);
                 System.exit(-1);
            }
        if(threshold<0)
            {
            	 String displayMsg = "Threshold must be greater or equal to 0";
                 System.out.println(displayMsg);
                 System.exit(-1);
            }
        //Step 1: Initialize MPI
        MPI.Init(InputArray) ;
	
        //Get the rank of the current process
        int myrank = MPI.COMM_WORLD.Rank() ;
        int p = MPI.COMM_WORLD.Size() ;
        long startIOTime = 0;
        long endIOTime = 0;
     	 
	//Step2: variable definitions
        Integer numUrls=-1;
	Integer totalNumUrls=0;
	HashMap <Integer,ArrayList<Integer>> adjacencyMatrix=new HashMap();
        ArrayList<Integer> amIndex= new ArrayList();
	String mpi_name=MPI.Get_processor_name();
       
        //Step3: Read the adjacency matrix file and the get the local parition
        try
        {
             startIOTime = System.currentTimeMillis();
            MPIRead.mpi_read(InputArray[3],amIndex,adjacencyMatrix,MPI.COMM_WORLD);
             endIOTime = System.currentTimeMillis();
        }
        catch (Exception ex)
        {
            System.out.println(InputArray[3]);
            ex.printStackTrace();
        }

        //Step4: Get the total number of URLS
        numUrls = (amIndex.size()/2);
        int[] numUrlsArray = new int[1];
        int[] totalNumUrlsArray = new int[1];
        numUrlsArray[0] = numUrls;
        //totalNumUrlsArray[0] = totalNumUrls;
        MPI.COMM_WORLD.Allreduce(numUrlsArray,0,totalNumUrlsArray,0,1,MPI.INT, MPI.SUM);
        totalNumUrls = totalNumUrlsArray[0];
       
        //Step5: Calculate the page ranks for each partition
        double[] rankValuesTable=new double[totalNumUrls] ;
        long startTime=0,endTime=0;
        if(myrank == 0)
           startTime = System.currentTimeMillis();
        MPIPageRank.mpi_pagerank(adjacencyMatrix, amIndex, numUrls, totalNumUrls,numIterations, threshold, rankValuesTable,MPI.COMM_WORLD);
        if(myrank == 0)
            endTime = System.currentTimeMillis();
      
        if(myrank ==0)
        {
           // System.out.println("rank = "+myrank);
            /*System.out.println("Page Ranks array:");
            for(int i=0;i<totalNumUrls;i++)
            { System.out.print(i);
                System.out.println(" "+rankValuesTable[i]);
            }*/
        
        //write results to the output file
        long totalWriteTime = sortHashMap(rankValuesTable, InputArray[4]);
         System.out.println("PageRanks are calculated");
                  System.out.println("IO time:" + ((endIOTime - startIOTime) + totalWriteTime) / 1000.0D + " sec.");
                System.out.println("Job turn around time:" + (endTime - startTime) / 1000.0D + " sec.");
        }

       


        //Shutdown the MPI
         MPI.Finalize();



   
    }//main
 

public static long sortHashMap(double[] finalRankValues, String paramOutputFileName) {
      HashMap <Integer,Double> sortedReversedMap = new HashMap();
      HashMap <Integer,Double> passedMap = new HashMap();

      for(int i =0; i< finalRankValues.length ; i++ )
          passedMap.put(i, finalRankValues[i]);


      // Store HashMap Values to a List and sort them.
      List mapKeys = new ArrayList(passedMap.keySet());
      List mapValues = new ArrayList(passedMap.values());
      Collections.sort(mapValues);

        LinkedHashMap sortedMap =
            new LinkedHashMap();

        Iterator valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Object val = valueIt.next();
            Iterator keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                Object key = keyIt.next();
                String comp1 = passedMap.get(key).toString();
                String comp2 = val.toString();

                if (comp1.equals(comp2)){
                    passedMap.remove(key);
                    mapKeys.remove(key);
                    sortedMap.put((Integer)key, (Double)val);
                    break;
                }//If Ends

            }//While Ends

        }//While Ends

        //Reverse the order to get top 10 ranks
        List sortedKeys = new ArrayList(sortedMap.keySet());
        List sortedValues = new ArrayList(sortedMap.values());
        Collections.reverse(sortedValues);
        Collections.reverse(sortedKeys);
      //Writing top 10 ranks to file
       long startWriteTime = 0;
        long endWriteTime = 0;
        try
        {
          
          startWriteTime = System.currentTimeMillis();
          FileWriter fStream = new FileWriter(paramOutputFileName);
          BufferedWriter out = new BufferedWriter(fStream);
          out.write ("Page  Ranks \r\n");
          for (int i = 0; i < 10; i++) {
              out.write((Integer)sortedKeys.get(i) + "       " );
              out.write((Double)sortedValues.get(i)+ "\r\n");
          }
                
          out.close();
          endWriteTime = System.currentTimeMillis();
        }// Try Ends
       catch (IOException localIOException) {
            localIOException.printStackTrace();
       }// catch Ends
    
    return (startWriteTime-endWriteTime);
    }
	

  }
