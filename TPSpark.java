package bigdata;


import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.HBaseManagement.HBaseProg;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;

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
	
	
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseManagement.HBaseProg(), args);
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		 
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
		hexColorPairRDD.foreach((colors) -> {
			int rowLevel =0;
			int colLevel = 0;
			BufferedImage buffImg = new BufferedImage(1201, 1201, BufferedImage.TYPE_INT_ARGB);
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
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(buffImg, "png", baos);
			baos.flush();
			byte[] imgInByte = baos.toByteArray();
			baos.close();
			String[] tokens = colors._1().toString().split("/");
			System.out.println(tokens.length);
			String location = tokens[tokens.length -1].split(".")[0];
			String locY = location.substring(3);
			String locX = location.substring(0,3);
			int intLocX = Integer.parseInt(locX.substring(1));
			int intLocY = Integer.parseInt(locY.substring(1));
			if (locX.toLowerCase().charAt(0) == 's') {
				intLocX *= -1;
			}
			if (locY.toLowerCase().charAt(0) == 's') {
				intLocY *= -1;
			}
			HBaseProg.storeData(location, intLocX, intLocY, imgInByte);
		/*
			File f = new File("output.png");
			ImageIO.write(buffImg, "png", f);
		
		*/
		
		/* --------------------- IMAGE HANDLING -------------- */
		
		
		/* ---------------------------------------------------- */
		});
		System.exit(exitCode);
	}
	
}
