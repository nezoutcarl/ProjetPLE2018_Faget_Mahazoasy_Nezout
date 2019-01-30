package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
	private static final int NB_ZOOMS = 3;

	private static final int LATITUDE_RANGE  = 90;
	private static final int LONGITUDE_RANGE = 180;

	private static class ColorHandler {
		private static final Color GREEN_WATER_COLOR = new Color(0xb0f2b6);
		private static final Color YELLOW_COLOR      = new Color(0xffe12d);
		private static final Color WHITE_SNOW_COLOR  = new Color(0xfffafa);

		public static Color getGradient(Color color1, Color color2, float percentage) {
			int red = (int) (color1.getRed() * percentage + color2.getRed() * (1 - percentage));
			int green = (int) (color1.getGreen() * percentage + color2.getGreen() * (1 - percentage));
			int blue = (int) (color1.getBlue() * percentage + color2.getBlue() * (1 - percentage));
			return new Color(red, green, blue);
		}

		public static int getColor(int altitude) {
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

	public static byte[][] computePictureData(String row, BufferedImage bufferedImage) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(bufferedImage, "png", baos);
		baos.flush();
		byte[] image = baos.toByteArray();
		baos.close();
		String[] tokens = row.split("/");
		int y = Integer.parseInt(tokens[1]);
		int x = Integer.parseInt(tokens[2]);
		byte[][] data = {
				row.getBytes(),
				image,
				{ new Integer(x).byteValue() },
				{ new Integer(y).byteValue() }
		};
		return data;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseManagement.HBasePictures(), args);
		SparkConf conf = new SparkConf().setAppName("PLE Project");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaPairRDD<Text, IntArrayWritable> pairRDD = context.sequenceFile("hdfs://young:9000/user/bfaget/test_projet/", Text.class, IntArrayWritable.class);
		
		/** Images 1201x1201 **/
		/*JavaPairRDD<Text, IntArrayWritable> colorPairRDD = pairRDD.mapValues((IntArrayWritable x) -> {
			int[] list = new int[x.getArray().length];
			for (int i = 0; i < x.getArray().length; ++i) {
				list[i] = ColorHandler.getColor(x.getArray()[i]);
			}
			return new IntArrayWritable(list);
		});
		colorPairRDD.foreach((Tuple2<Text, IntArrayWritable> pair) -> {
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
			int zoom = 1;
			String location = computeLocation(pair._1.toString());
			int[] coordinates = computeCoordinates(location);
			String row = zoom + "/" + (coordinates[1] + LATITUDE_RANGE) + "/" + (coordinates[0] + LONGITUDE_RANGE);
			byte[][] data = computePictureData(row, bufferedImage);
			HBasePictures.storePictureData(data);
		});*/
		
		/** Images 256x256 **/
		JavaPairRDD<String, short[]> tmpPairRDD = pairRDD.mapToPair((Tuple2<Text, IntArrayWritable> pair) -> {
			int[] intArray = pair._2.getArray();
			short[] shortArray = new short[intArray.length];
			for(int i = 0; i < intArray.length; ++i) {
				shortArray[i] = (short)intArray[i];
			}
			return new Tuple2<>(pair._1.toString(), shortArray);
		});
		int numPartitions = conf.getInt("spark.executor.instances", 10);
		tmpPairRDD = tmpPairRDD.repartition(numPartitions);
		JavaPairRDD<String, short[][]> finalPairRDD = tmpPairRDD.flatMapToPair((Tuple2<String, short[]> pair) -> {
			List<Tuple2<String, short[][]>> list = new ArrayList<>();
			String location = computeLocation(pair._1);
			int[] coordinates = computeCoordinates(location);
			int y0 = (coordinates[1] + LATITUDE_RANGE) * INITIAL_SIDE_SIZE;
			int x0 = (coordinates[0] + LONGITUDE_RANGE) * INITIAL_SIDE_SIZE;
			Map<String, short[][]> map = new HashMap<>();
			for (int i = 0; i < pair._2.length; ++i) {
				int keyX = (x0 + i % INITIAL_SIDE_SIZE) / FINAL_SIDE_SIZE;
				int keyY = (y0 + i / INITIAL_SIDE_SIZE) / FINAL_SIDE_SIZE;
				String key = keyY + "/" + keyX;
				short coordX = (short) ((x0 + i % INITIAL_SIDE_SIZE) % FINAL_SIDE_SIZE);
				short coordY = (short) ((y0 + i / INITIAL_SIDE_SIZE) % FINAL_SIDE_SIZE);
				short altitude = (short) pair._2[i];
				short[][] values = map.get(key);
				if (values != null) {
					values[0] = ArrayUtils.add(values[0], coordX);
					values[0] = ArrayUtils.add(values[0], coordY);
					values[1] = ArrayUtils.add(values[1], altitude);
					map.replace(key, values);
				}
				else {
					values = new short [2][0];
					values[0] = ArrayUtils.add(values[0], coordX);
					values[0] = ArrayUtils.add(values[0], coordY);
					values[1] = ArrayUtils.add(values[1], altitude);
					map.put(key, values);
				}
			}
			for (String key: map.keySet()) {
				list.add(new Tuple2<>(key.toString(), map.get(key)));
			}
			return list.iterator();
		});
		finalPairRDD = finalPairRDD.reduceByKey((short[][] values1, short[][] values2) -> {
			values1[0] = ArrayUtils.addAll(values1[0], values2[0]);
			values1[1] = ArrayUtils.addAll(values1[1], values2[1]);
			return values1;
		});
		finalPairRDD.foreach((Tuple2<String, short[][]> pair) -> {
			BufferedImage bufferedImage = new BufferedImage(FINAL_SIDE_SIZE, FINAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2[1].length; ++i) {
				bufferedImage.setRGB(pair._2[0][2 * i] % FINAL_SIDE_SIZE, pair._2[0][2 * i + 1] / FINAL_SIDE_SIZE, ColorHandler.getColor(pair._2[1][i]));
			}
			int zoom = 1;
			String row = zoom + "/" + pair._1;
			byte[][] data = computePictureData(row, bufferedImage);
			HBasePictures.storePictureData(data);
		});
		
		/** Images 256x256 + 3 niveaux de zoom **/
		/*JavaPairRDD<String, short[]> tmpPairRDD = pairRDD.mapToPair((Tuple2<Text, IntArrayWritable> pair) -> {
			int[] intArray = pair._2.getArray();
			short[] shortArray = new short[intArray.length];
			for(int i = 0; i < intArray.length; ++i) {
				shortArray[i] = (short)intArray[i];
			}
			return new Tuple2<>(pair._1.toString(), shortArray);
		});
		int numPartitions = conf.getInt("spark.executor.instances", 10);
		tmpPairRDD = tmpPairRDD.repartition(numPartitions);
		JavaPairRDD<String, short[][]> finalPairRDD = tmpPairRDD.flatMapToPair((Tuple2<String, short[]> pair) -> {
			List<Tuple2<String, short[][]>> list = new ArrayList<>();
			String location = computeLocation(pair._1);
			int[] coordinates = computeCoordinates(location);
			int y0 = (coordinates[1] + LATITUDE_RANGE) * INITIAL_SIDE_SIZE;
			int x0 = (coordinates[0] + LONGITUDE_RANGE) * INITIAL_SIDE_SIZE;
			Map<String, short[][]> map = new HashMap<>();
			for (int z = 1; z <= NB_ZOOMS; ++z) {
				for (int i = 0; i < pair._2.length; ++i) {
					int zoom = (int) Math.pow(2, z);
					int keyX = (x0 + i % INITIAL_SIDE_SIZE) / (FINAL_SIDE_SIZE * zoom);
					int keyY = (y0 + i / INITIAL_SIDE_SIZE) / (FINAL_SIDE_SIZE * zoom);
					String key = z + "/" + keyY + "/" + keyX;
					short coordX = (short) ((x0 + i % INITIAL_SIDE_SIZE) % (FINAL_SIDE_SIZE * zoom));
					short coordY = (short) ((y0 + i / INITIAL_SIDE_SIZE) % (FINAL_SIDE_SIZE * zoom));
					short altitude = (short) pair._2[i];
					short[][] values = map.get(key);
					if (values != null) {
						values[0] = ArrayUtils.add(values[0], coordX);
						values[0] = ArrayUtils.add(values[0], coordY);
						values[1] = ArrayUtils.add(values[1], altitude);
						map.replace(key, values);
					}
					else {
						values = new short [2][0];
						values[0] = ArrayUtils.add(values[0], coordX);
						values[0] = ArrayUtils.add(values[0], coordY);
						values[1] = ArrayUtils.add(values[1], altitude);
						map.put(key, values);
					}
				}
			}
			for (String key: map.keySet()) {
				list.add(new Tuple2<>(key.toString(), map.get(key)));
			}
			return list.iterator();
		});
		finalPairRDD = finalPairRDD.reduceByKey((short[][] values1, short[][] values2) -> {
			values1[0] = ArrayUtils.addAll(values1[0], values2[0]);
			values1[1] = ArrayUtils.addAll(values1[1], values2[1]);
			return values1;
		});
		finalPairRDD.foreach((Tuple2<String, short[][]> pair) -> {
			String[] tokens = pair._1.split("/");
			int z = Integer.parseInt(tokens[0]);
			int zoom = (int) Math.pow(2, z);
			BufferedImage bufferedImage = new BufferedImage(FINAL_SIDE_SIZE * zoom, FINAL_SIDE_SIZE * zoom, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2[1].length; ++i) {
				bufferedImage.setRGB(pair._2[0][2 * i] % (FINAL_SIDE_SIZE * zoom), pair._2[0][2 * i + 1] / (FINAL_SIDE_SIZE * zoom), ColorHandler.getColor(pair._2[1][i]));
			}
			if (z > 1) {
				Image tmpImage = bufferedImage.getScaledInstance(FINAL_SIDE_SIZE * zoom, FINAL_SIDE_SIZE * zoom, Image.SCALE_FAST);
		        BufferedImage resizedImage = new BufferedImage(FINAL_SIDE_SIZE, FINAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
		        Graphics2D g2d = resizedImage.createGraphics();
		        g2d.drawImage(tmpImage, 0, 0, null);
		        g2d.dispose();
		        bufferedImage = resizedImage;
			}
			byte[][] data = computePictureData(pair._1, bufferedImage);
			HBasePictures.storePictureData(data);
		});*/

		context.close();
		System.exit(exitCode);
	}	
}
