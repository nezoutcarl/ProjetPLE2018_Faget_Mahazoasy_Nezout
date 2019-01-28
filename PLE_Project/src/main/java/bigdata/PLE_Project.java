package bigdata;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.HBaseManagement.HBasePictures;
import bigdata.IntArrayWritable;
import scala.Tuple2;

public class PLE_Project {
	private static final int INTERMEDIATE_ALTITUDE = 4000;
	private static final int MAXIMUM_ALTITUDE      = 8000;
	private static final int MINIMUM_ALTITUDE      = 0;

	private static final int INITIAL_SIDE_SIZE = 1201;
	private static final int FINAL_SIDE_SIZE   = 256;

	private static final int LATITUDE_RANGE  = 90;
	private static final int LONGITUDE_RANGE = 180;

	private static class ColorHandler {
		private static final Color GREEN_WATER_COLOR = new Color(0xb0f2b6);
		private static final Color YELLOW_COLOR      = new Color(0xffe12d);
		private static final Color WHITE_SNOW_COLOR  = new Color(0xfffafa);

		public static Color getGradient(Color color1, Color color2, float percentage) {
			int red = (int)(color1.getRed() * percentage + color2.getRed() * (1 - percentage));
			int green = (int)(color1.getGreen() * percentage + color2.getGreen() * (1 - percentage));
			int blue = (int)(color1.getBlue() * percentage + color2.getBlue() * (1 - percentage));
			return new Color(red, green, blue);
		}

		public static int getColor(short altitude) {
			Color color = null;
			if (altitude <= INTERMEDIATE_ALTITUDE) {
				int range = INTERMEDIATE_ALTITUDE - MINIMUM_ALTITUDE;
				float percentage = (altitude - MINIMUM_ALTITUDE) / range;
				color = getGradient(YELLOW_COLOR, GREEN_WATER_COLOR, percentage);
			}
			else {
				int range = MAXIMUM_ALTITUDE - INTERMEDIATE_ALTITUDE;
				float percentage = (altitude - INTERMEDIATE_ALTITUDE) / range;
				color = getGradient(WHITE_SNOW_COLOR, YELLOW_COLOR, percentage);
			}
			return color.getRGB();
		}
	}

	public static String computeLocation(String path) {
		String[] tokens = path.split("/");
		String location = tokens[tokens.length - 1].split("\\.")[0];
		return location;
	}

	public static int[] computeCoordinates(String location) {
		String locationY = location.substring(0, 3);
		String locationX = location.substring(3);
		int y = Integer.parseInt(locationY.substring(1));
		int x = Integer.parseInt(locationX.substring(1));
		if (locationY.toLowerCase().charAt(0) == 's') {
			y *= -1;
		}
		if (locationX.toLowerCase().charAt(0) == 'w') {
			x *= -1;
		}
		int[] result = { x, y };
		return result;
	}

	public static byte[][] computePictureData(String location, BufferedImage bufferedImage, int zoom) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(bufferedImage, "png", baos);
		baos.flush();
		byte[] image = baos.toByteArray();
		baos.close();
		String[] tokens = location.split("/");
		int y = Integer.parseInt(tokens[0]);
		int x = Integer.parseInt(tokens[1]);
		byte[][] data = {
				location.getBytes(),
				image,
				{ new Integer(x).byteValue() },
				{ new Integer(y).byteValue() },
				{ new Integer(zoom).byteValue() }
		};
		return data;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseManagement.HBasePictures(), args);
		SparkConf conf = new SparkConf().setAppName("PLE Project");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaPairRDD<Text, IntArrayWritable> pairRDD = context.sequenceFile("hdfs://young:9000/user/bfaget/test_projet/", Text.class, IntArrayWritable.class);
		/*JavaPairRDD<Text, IntArrayWritable> colorPairRDD = pairRDD.mapValues((IntArrayWritable x) -> {
			int[] list = new int[x.getArray().length];
			for (int i = 0; i < x.getArray().length; ++i) {
				list[i] = ColorHandler.getColor(x.getArray()[i]);
			}
			return new IntArrayWritable(list);
		});*/
		/*colorPairRDD.foreach((Tuple2<Text, IntArrayWritable> pair) -> {
			int rowIndex = 0;
			int columnIndex = 0;
			BufferedImage bufferedImage = new BufferedImage(INITIAL_SIDE_SIZE, INITIAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2.getArray().length; ++i) {
				bufferedImage.setRGB(rowIndex, columnIndex, new Color(pair._2.getArray()[i]).getRGB());
				if ((i + 1) % INITIAL_SIDE_SIZE == 0) {
					columnIndex = 0;
					rowIndex++;
				}
				else {
					columnIndex++;
				}
			}
			int zoom = -1;
			byte[][] data = computePictureData(pair._1.toString(), bufferedImage, zoom);
			HBasePictures.storePictureData(data);
		});*/
		int numPartitions = conf.getInt("spark.executor.instances", 10);
		JavaPairRDD<String, short[]> tmpPairRDD = pairRDD.mapToPair((Tuple2<Text, IntArrayWritable> pair) -> {
			int[] intArray = pair._2.getArray();
			short[] shortArray = new short[intArray.length];
			for(int i = 0; i < intArray.length; ++i) {
				shortArray[i] = (short)intArray[i];
			}
			return new Tuple2<String, short[]>(pair._1.toString(), shortArray);
		});
		tmpPairRDD = tmpPairRDD.repartition(numPartitions);
		JavaPairRDD<String, Tuple2<short[][], short[]>> finalPairRDD = tmpPairRDD.flatMapToPair((Tuple2<String, short[]> pair) -> {
			List<Tuple2<String, Tuple2<short[][], short[]>>> list = new ArrayList<Tuple2<String, Tuple2<short[][], short[]>>>();
			String location = computeLocation(pair._1.toString());
			int[] coordinates = computeCoordinates(location);
			int y0 = (coordinates[1] + LATITUDE_RANGE) * INITIAL_SIDE_SIZE;
			int x0 = (coordinates[0] + LONGITUDE_RANGE) * INITIAL_SIDE_SIZE;
			Map<String, Tuple2<short[][], short[]>> map = new HashMap<String, Tuple2<short[][], short[]>>();
			for (int i = 0; i < pair._2.length; ++i) {
				int keyX = (x0 + i % INITIAL_SIDE_SIZE) / FINAL_SIDE_SIZE;
				int keyY = (y0 + i / INITIAL_SIDE_SIZE) / FINAL_SIDE_SIZE;
				String key = keyY + "/" + keyX;
				int coordX = (x0 + i % INITIAL_SIDE_SIZE) % FINAL_SIDE_SIZE;
				int coordY = (y0 + i / INITIAL_SIDE_SIZE) % FINAL_SIDE_SIZE;
				short altitude = pair._2[i];
				Tuple2<short[][], short[]> values = map.get(key);
				if (values != null) {
					short[] coordsX = ArrayUtils.add(values._1[0], (short)coordX);
					short[] coordsY = ArrayUtils.add(values._1[1], (short)coordY);
					short[][] coords = { coordsX, coordsY };
					short[] altitudes = ArrayUtils.add(values._2, altitude);
					values = new Tuple2<short[][], short[]>(coords, altitudes);
					map.replace(key, values);
				}
				else {
					short[] coordsX = { (short)coordX };
					short[] coordsY = { (short)coordY };
					short[][] coords = { coordsX, coordsY };
					short[] altitudes = { altitude };
					values = new Tuple2<short[][], short[]>(coords, altitudes);
					map.put(key, values);
				}
			}
			for (String key: map.keySet()) {
				list.add(new Tuple2<String, Tuple2<short[][], short[]>>(key.toString(), map.get(key)));
			}
			return list.iterator();
		});
		finalPairRDD = finalPairRDD.reduceByKey((Tuple2<short[][], short[]> values1, Tuple2<short[][], short[]> values2) -> {
			short[] coordsX = ArrayUtils.addAll(values1._1[0], values2._1[0]);
			short[] coordsY = ArrayUtils.addAll(values1._1[1], values2._1[1]);
			short[][] coords = { coordsX, coordsY };
			short[] altitudes = ArrayUtils.addAll(values1._2, values1._2);
			return new Tuple2<short[][], short[]>(coords, altitudes);
		});
		finalPairRDD.cache();
		Iterator<Tuple2<String, Tuple2<short[][], short[]>>> it = finalPairRDD.collect().iterator();
		while(it.hasNext()) {
			Tuple2<String, Tuple2<short[][], short[]>> pair = it.next();	
			//System.out.println("###pair._2._1 length x:"+ pair._2._1[0].length + " y:" + pair._2._1[1].length);
			for (int i = 0; i < pair._2._1[0].length; ++i) {
				System.out.println("-----");
				if(pair._2._1[0][i] >= 256 ||  pair._2._1[1][i] >=256 || pair._2._1[0][i] < 0 ||  pair._2._1[1][i] <0)
					System.out.println("###pair._2._1[0] x:"+ pair._2._1[0][i] + " y:" + pair._2._1[1][i]);
			}
		} 
		/*
		finalPairRDD.foreach((Tuple2<String, Tuple2<short[][], short[]>> pair) -> {
			BufferedImage bufferedImage = new BufferedImage(FINAL_SIDE_SIZE, FINAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2._1[0].length; ++i) {
				bufferedImage.setRGB(pair._2._1[0][i], pair._2._1[1][i], new Color(ColorHandler.getColor(pair._2._2[i])).getRGB());
			}
			int zoom = -1;
			byte[][] data = computePictureData(pair._1, bufferedImage, zoom);
			HBasePictures.storePictureData(data);
		});*/
		context.close();
		System.exit(exitCode);
	}
	
}
