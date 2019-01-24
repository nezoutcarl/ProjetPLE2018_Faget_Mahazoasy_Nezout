~~~~
package bigdata;


import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.lib.input.*;



public class TPSpark {
	final static int SIDE_SIZE = 1201;
	public static class ColorHandling {
		 
		private static final Color GREEN_WATER_COLOR = new Color(0xb0f2b6);
		private static final Color YELLOW_COLOR = new Color(0xfce903);
		private static final Color WHITE_SNOW_COLOR = new Color(0xfcfcfc);
		
		
		public static Color getColor(int altitude) {
			Color res;
			if(altitude<= 4000) {
				if( altitude != 4000)
					res=WHITE_SNOW_COLOR;
				else
					res=YELLOW_COLOR;
			}
			else
				res=GREEN_WATER_COLOR;
			return res;
		}
		
		public static int getHexColor(int altitude) {
			int hexColor ;
			if(altitude<= 4000) {
				if( altitude != 4000)
					hexColor=0xfcfcfc;
				else
					hexColor=0xfce903;
			}
			else
				hexColor=0xb0f2b6;
			
			return hexColor;			
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		Configuration confHBase = HBaseConfiguration.create();
		confHBase.set("hbase.zookeeper.quorum", "");
		//JavaHBaseContext hbaseContext = new JavaHBaseContext(context, confHBase);
		//confHBase.set(TableInputFormat.INPUT_TABLE, "tableName");

		// Number of partitions
		int numExecutors = conf.getInt("spark.executor.instances", 1);
		// Ex1
		//JavaRDD<String> textFile = context.textFile("hdfs://young:9000/user/raw_data/worldcitiespop.txt", numExecutors);
		//textFile.saveAsTextFile("hdfs://young:9000/user/cnezout/myResult.txt");
		JavaPairRDD<Text, IntArrayWritable> pairRDD = context.sequenceFile("hdfs://young:9000/user/cnezout001/dem3Light/", Text.class, IntArrayWritable.class);
	
	
		// Traitement des colorations
		JavaPairRDD<Text,int[]> hexColorPairRDD = pairRDD.mapValues((x)->{
			int[] lt= new int[x.getArray().length];
			for(int i=0; i<x.getArray().length; i++) {
				lt[i]=ColorHandling.getHexColor(x.getArray()[i]);
			}
			return lt;
		});
		//traitement des images générées puis envoyées dans la table HBase
		hexColorPairRDD.foreach(colors -> {
			int rowLevel =0;
			int colLevel = 0;
			BufferedImage buffImg = new BufferedImage(SIDE_SIZE, SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for(int i =0; i< colors._2.length; i++) {
				buffImg.setRGB(rowLevel, colLevel, new Color(colors._2[i]).getRGB());
				if(((i+1) % 1201 == 0) && (i != 0)) {
					rowLevel++;
					colLevel=0;
				}
				else
					colLevel++;
			}		
			//TODO Put image on HBASE with his location path
		});
		/*
			File f = new File("output.png");
			ImageIO.write(buffImg, "png", f);
		
		*/
		
		/* --------------------- IMAGE HANDLING -------------- */
		
		
		/* ---------------------------------------------------- */
		hexColorPairRDD.saveAsTextFile("hdfs://young:9000/user/cnezout001/colorJob/");
		
	}
	
}
~~~~
