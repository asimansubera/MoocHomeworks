import mpi.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.FileInputStream;
import java.io.File;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Collections;

                                                                     
                                                                     
                                                                     
                                       

//import javax.swing.text.html.HTMLDocument.Iterator;



public class MPIRead
{

	public static void mpi_read (String fileName, ArrayList<Integer> amIndex,
                HashMap<Integer,ArrayList<Integer>> adjacencyMatrix,Intracomm comm) throws IOException
    	
	{
	    int i, j, k, len, numberOfDivisions, remainder;
	    Integer totalNumUrls = 0;
	    int myRank = comm.Rank() ;
	    int nProcess = comm.Size() ;
	    
	    if (myRank == 0)
	    {
                
                //Check if file is empty
               FileInputStream fis = new FileInputStream(new File(fileName));
               int result = fis.read();
               if(result == -1)
                   System.exit(-1);

	    	initializeAdjacencyMatrix(adjacencyMatrix,fileName);
	    	totalNumUrls = adjacencyMatrix.size();
     
                //int n = nProcess-1;
                numberOfDivisions = (totalNumUrls) / nProcess;
                remainder = (totalNumUrls) % nProcess;
	        int block_size;       
	        int send_start_index;  
	        /* scatter adjacency_matrix[total_num_urls] to other processes */
	        List mapKeys = new ArrayList(adjacencyMatrix.keySet());
	        Iterator localIterator = mapKeys.iterator();

	        int  v=k=0;
	        for (i = 0; i < nProcess; i++)
	        {       	
                    send_start_index = k;
                    //block_size = (i < (nProcess-1)) ? numberOfDivisions: (numberOfDivisions + remainder);
                    if(remainder==0)
                        block_size = numberOfDivisions;
                    else
                        block_size = (i < remainder) ?numberOfDivisions + 1: (numberOfDivisions);
                   //System.out.println("i="+i+","+"block size="+ block_size);
                    int [] send_amIndex =new int[block_size*2];
                    int index=0;
	            for (k = send_start_index; k <= ((send_start_index+block_size)-1); k++)
	            {
                        int sourceUrl = (Integer)localIterator.next();
                         ArrayList<Integer> targetUrlList =  adjacencyMatrix.get(sourceUrl);
                         int targetSize= targetUrlList.size();
                         send_amIndex[index++]= sourceUrl;
                         send_amIndex[index++]= targetSize;
                    }//for k
                        //send send_amIndex
                        int[] send_amIndexSize= new int[1];
                         send_amIndexSize[0]=block_size*2;
                       if(i ==0)
                       {
                           for (int x=0; x<send_amIndexSize[0];x++)
                            amIndex.add(send_amIndex[x]);
                          
                       }
                       else
                       {
                         comm.Send(send_amIndexSize, 0, 1, MPI.INT, i, 0);
                         comm.Send(send_amIndex, 0, send_amIndexSize[0], MPI.INT, i, 1);
                       }

                         //send the targetUrls for each page
                         for(int x=0;x<block_size*2;)
                         {
                             int sourceUrl = send_amIndex[x++];
                             int targetSize = send_amIndex[x++];
                             int[] send_targetList= new int[targetSize];
                             ArrayList<Integer> targetList = adjacencyMatrix.get(sourceUrl);
                             for(int y=0;y<targetSize;y++)
                                 send_targetList[y]=targetList.get(y);
                             /*System.out.println("send source="+sourceUrl);
                             System.out.println("send targetszie="+targetSize);*/
                             if(i==0)
                             {
                                 for(int y=0;y<targetSize;y++)
                                    targetList.add(send_targetList[y]);

                                 adjacencyMatrix.put(sourceUrl, targetList);
                             }
                             else
                             {
                                comm.Send(send_targetList, 0, targetSize, MPI.INT, i, 2);
                             }
                         }//for x
                }// for i nProcess
	    }//if rank=0
            else
            {
                //step1: receive the amIndex size
                int[] recv_amIndexSize= new int[1];
                comm.Recv(recv_amIndexSize, 0, 1, MPI.INT, 0, 0);
                //step2: receive the amIndex array
                int[] recv_amIndex= new int[recv_amIndexSize[0]];
                comm.Recv(recv_amIndex, 0, recv_amIndexSize[0], MPI.INT, 0, 1);
                for (int x=0; x<recv_amIndexSize[0];x++)
                    amIndex.add(recv_amIndex[x]);
                //step3: receive the target lists of each page
                int numUrls = recv_amIndexSize[0]/2;
                for(int x=0;x<numUrls*2;)
                 {
                     int sourceUrl = recv_amIndex[x++];
                     int targetSize = recv_amIndex[x++];
                     int[] recv_targetList= new int[targetSize];
                     comm.Recv(recv_targetList, 0, targetSize, MPI.INT, 0, 2);
                     ArrayList<Integer> targetList = new ArrayList();                    
                     for(int y=0;y<targetSize;y++)
                           targetList.add(recv_targetList[y]);
                    
                    adjacencyMatrix.put(sourceUrl, targetList);
                 }//for each page
	    }//else
		
		
}//func
	
	
	public static void initializeAdjacencyMatrix (HashMap<Integer,ArrayList<Integer>> paramAdjacencyMatrix,String inputFileName)
	  
	{
	 try
	     {
             
	      FileReader localFileReader = new FileReader(inputFileName);
	      BufferedReader localBufferedReader = new BufferedReader(localFileReader);
	     String strRead;
	       while ((strRead = localBufferedReader.readLine()) != null) {
	        String[] arrayOfString = strRead.split(" ");
	         int i = Integer.parseInt(arrayOfString[0]);
	        ArrayList localArrayList = new ArrayList();
	        for (int k = 1; k < arrayOfString.length; k++) {
	           int j = Integer.parseInt(arrayOfString[k]);
	          localArrayList.add(Integer.valueOf(j));
	         }// For Ends
	        paramAdjacencyMatrix.put(Integer.valueOf(i), localArrayList);
	      }//While Ends
	       //System.out.println ("Adjacency Matrix initialized");
	     } //Try Ends
	    catch (IOException localIOException) {
	    	localIOException.printStackTrace(); 
	    }// catch Ends
	  }//initializeAdjacencyMatrix Ends



	  
	
}
