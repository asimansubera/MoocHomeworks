package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	//each map task handle one line within adjacency matrix file
	// key: file offset
	// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		
		int numUrls = context.getConfiguration().getInt("numUrls",1);
		
		String line = value.toString();
		//instance an object that record the information for one web page
		RankRecord rrd = new RankRecord(line);
		
		int sourceUrl, targetUrl;
		//double rankValueOfSrcUrl;
		
		if (rrd.targetUrlsList.size()<=0){//there is no out degree for this web page, scatter its rank value to all other urls
			double rankValuePerUrl = rrd.rankValue/(double)numUrls;
			for (int i=0;i<numUrls;i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
			}
		}else{  
			StringBuffer sb = new StringBuffer();
			// Write your code here
double rankValuePerTargetUrl = rrd.rankValue/(double)(rrd.targetUrlsList.size());
			//sb.append(rrd.sourceUrl);
			sb.append(String.valueOf(0.0f));
			for (int i=0;i<rrd.targetUrlsList.size();i++){
				targetUrl = rrd.targetUrlsList.get(i);
				
				sb.append("#"+targetUrl);
				context.write(new LongWritable(targetUrl), new Text(String.valueOf(rankValuePerTargetUrl)));
			}//for
			context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));
		}
	}//map
}
