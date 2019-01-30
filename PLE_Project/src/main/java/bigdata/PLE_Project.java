package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
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
import org.apache.spark.input.PortableDataStream;

import bigdata.HBaseManagement.HBasePictures;
import bigdata.IntArrayWritable;
import scala.Tuple2;

public class PLE_Project {
	private static final int INTERMEDIATE_ALTITUDE = 4000;
	private static final int MAXIMUM_ALTITUDE = 8000;
	private static final int MINIMUM_ALTITUDE = 0;

	private static final int FINAL_SIDE_SIZE = 256;
	private static final int INITIAL_SIDE_SIZE = 1201;
	private static final int ZOOM_LEVELS = 3;

	private static final int LATITUDE_RANGE = 90;
	private static final int LONGITUDE_RANGE = 180;

	private static class ColorHandler {
		private static final Color BROWN_COLOR = new Color(0x826628);
		public static final Color ERROR_COLOR = new Color(0xff4862);
		private static final Color GREEN_COLOR = new Color(0x16640d);
		private static final Color YELLOW_COLOR = new Color(0xf6f6a2);
		public static final Color ZERO_COLOR = new Color(0x87cefa);

		private static final int MAX_RGB = 255;
		private static final int MIN_RGB = 0;

		public static Color getGradient(Color color1, Color color2, float percentage) {
			float inverse_percentage = 1 - percentage;
			float red = color1.getRed() * percentage + color2.getRed() * inverse_percentage;
			float green = color1.getGreen() * percentage + color2.getGreen() * inverse_percentage;
			float blue = color1.getBlue() * percentage + color2.getBlue() * inverse_percentage;
			red = ((red < 0) ? MIN_RGB : ((red > MAX_RGB) ? MAX_RGB : red)) / MAX_RGB;
			blue = ((blue < 0) ? MIN_RGB : ((blue > MAX_RGB) ? MAX_RGB : blue)) / MAX_RGB;
			green = ((green < 0) ? MIN_RGB : ((green > MAX_RGB) ? MAX_RGB : green)) / MAX_RGB;
			return new Color(red, green, blue);
		}

		public static int getColor(int altitude) {
			Color color = null;
			if (altitude <= MINIMUM_ALTITUDE) {
				color = ZERO_COLOR;
			}
			else if (altitude <= INTERMEDIATE_ALTITUDE) {
				float range = INTERMEDIATE_ALTITUDE - MINIMUM_ALTITUDE;
				float percentage = (float) (altitude - MINIMUM_ALTITUDE) / range;
				color = getGradient(YELLOW_COLOR, GREEN_COLOR, percentage);
			}
			else if (altitude <= MAXIMUM_ALTITUDE) {
				int range = MAXIMUM_ALTITUDE - INTERMEDIATE_ALTITUDE;
				float percentage = (altitude - INTERMEDIATE_ALTITUDE) / range;
				color = getGradient(BROWN_COLOR, YELLOW_COLOR, percentage);
			}
			else {
				color = ERROR_COLOR;
			}
			return color.getRGB();
		}
	}

	private static class ImageHandler {
		public static BufferedImage resizeImage(BufferedImage bufferedImage, int zoom) {
			Image tmpImage = bufferedImage.getScaledInstance(FINAL_SIDE_SIZE * zoom, FINAL_SIDE_SIZE * zoom, Image.SCALE_FAST);
	        BufferedImage resizedImage = new BufferedImage(FINAL_SIDE_SIZE, FINAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
	        Graphics2D g2d = resizedImage.createGraphics();
	        g2d.drawImage(tmpImage, 0, 0, null);
	        g2d.dispose();
	        return resizedImage;
		}
	}

	public static String computeLocation(String path) {
		String[] tokens = path.split("/");
		String location = tokens[tokens.length - 1].split("\\.")[0];
		return location;
	}

	public static int[] computeCoordinates(String location) {
		String locY = location.substring(0, 3);
		String locX = location.substring(3);
		int y = Integer.parseInt(locY.substring(1));
		int x = Integer.parseInt(locX.substring(1));
		if (locY.toLowerCase().charAt(0) == 's') {
			y *= -1;
		}
		if (locX.toLowerCase().charAt(0) == 'w') {
			x *= -1;
		}
		y += LATITUDE_RANGE;
		x += LONGITUDE_RANGE;
		int[] result = { y, x };
		return result;
	}

	public static byte[][] computePictureData(String row, BufferedImage bufferedImage) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(bufferedImage, "png", baos);
		baos.flush();
		byte[] image = baos.toByteArray();
		baos.close();
		byte[][] data = {
				row.getBytes(),
				image
		};
		return data;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseManagement.HBasePictures(), args);
		SparkConf conf = new SparkConf().setAppName("PLE Project");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaPairRDD<String, PortableDataStream> pairRDD = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/");

		/** Images 1201x1201 (.hgt) **/
		JavaPairRDD<String, int[]> colorPairRDD = pairRDD.mapToPair((Tuple2<String, PortableDataStream> pair) -> {
			DataInputStream dis = pair._2.open();
			byte[] data = new byte[dis.available()];
			dis.readFully(data);
            IntBuffer ib = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).asIntBuffer();
            int[] array = new int[ib.remaining()];
            ib.get(array);
            int[] list = new int[array.length];
			for (int i = 0; i < array.length; ++i) {
				list[i] = ColorHandler.getColor(array[i]);
			}
			return new Tuple2<String, int[]>(pair._1.toString(), list);
		});
		int numPartitions = conf.getInt("spark.executor.instances", 10) * conf.getInt("spark.executor.cores", 10);
		colorPairRDD = colorPairRDD.repartition(numPartitions);
		colorPairRDD.foreach((Tuple2<String, int[]> pair) -> {
			BufferedImage bufferedImage = new BufferedImage(INITIAL_SIDE_SIZE, INITIAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2.length; ++i) {
				int y = i % INITIAL_SIDE_SIZE;
				int x = i / INITIAL_SIDE_SIZE;
				int color = pair._2[i];
				bufferedImage.setRGB(x, y, color);
			}
			String location = computeLocation(pair._1.toString());
			int[] coordinates = computeCoordinates(location);
			int posY = coordinates[0];
			int posX = coordinates[1];
			int zoom = 1;
			String row = zoom + "/" + posY + "/" + posX;
			byte[][] data = computePictureData(row, bufferedImage);
			HBasePictures.storePictureData(data);
		});
		
		//JavaPairRDD<Text, IntArrayWritable> pairRDD = context.sequenceFile("hdfs://young:9000/user/bfaget/projet/", Text.class, IntArrayWritable.class);
		
		/** Images 1201x1201 (SequenceFile) **/
		/*
		JavaPairRDD<String, int[]> colorPairRDD = pairRDD.mapToPair((Tuple2<Text, IntArrayWritable> pair) -> {
			int[] list = new int[pair._2.getArray().length];
			for (int i = 0; i < pair._2.getArray().length; ++i) {
				list[i] = ColorHandler.getColor(pair._2.getArray()[i]);
			}
			return new Tuple2<String, int[]>(pair._1.toString(), list);
		});
		int numPartitions = conf.getInt("spark.executor.instances", 10);
		colorPairRDD = colorPairRDD.repartition(numPartitions);
		colorPairRDD.foreach((Tuple2<String, int[]> pair) -> {
			BufferedImage bufferedImage = new BufferedImage(INITIAL_SIDE_SIZE, INITIAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2.length; ++i) {
				int y = i % INITIAL_SIDE_SIZE;
				int x = i / INITIAL_SIDE_SIZE;
				int color = pair._2[i];
				bufferedImage.setRGB(x, y, color);
			}
			String location = computeLocation(pair._1.toString());
			int[] coordinates = computeCoordinates(location);
			int posY = coordinates[0];
			int posX = coordinates[1];
			int zoom = 1;
			String row = zoom + "/" + posY + "/" + posX;
			byte[][] data = computePictureData(row, bufferedImage);
			HBasePictures.storePictureData(data);
		});
		*/

		/** Images 256x256 **/
		/*
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
			String location = computeLocation(pair._1.toString());
			int[] coordinates = computeCoordinates(location);
			int y0 = coordinates[0] * INITIAL_SIDE_SIZE;
			int x0 = coordinates[1] * INITIAL_SIDE_SIZE;
			Map<String, short[][]> map = new HashMap<>();
			for (int i = 0; i < pair._2.length; ++i) {
				int posY = y0 + i / INITIAL_SIDE_SIZE;
				int posX = x0 + i % INITIAL_SIDE_SIZE;
				short coordY = (short) (posY % FINAL_SIDE_SIZE);
				short coordX = (short) (posX % FINAL_SIDE_SIZE);
				short altitude = (short) pair._2[i];
				int keyY = posY / FINAL_SIDE_SIZE;
				int keyX = posX / FINAL_SIDE_SIZE;
				String key = keyY + "/" + keyX;
				short[][] values = map.get(key);
				if (values != null) {
					values[0] = ArrayUtils.add(values[0], coordY);
					values[0] = ArrayUtils.add(values[0], coordX);
					values[1] = ArrayUtils.add(values[1], altitude);
					map.replace(key, values);
				}
				else {
					values = new short [2][0];
					values[0] = ArrayUtils.add(values[0], coordY);
					values[0] = ArrayUtils.add(values[0], coordX);
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
				int y = pair._2[0][2 * i];
				int x = pair._2[0][2 * i + 1];
				int altitude = pair._2[1][i];
				int color = ColorHandler.getColor(altitude);
				bufferedImage.setRGB(x, y, color);
			}
			int zoom = 1;
			String row = zoom + "/" + pair._1;
			byte[][] data = computePictureData(row, bufferedImage);
			HBasePictures.storePictureData(data);
		});
		*/

		/** Images 256x256 + Niveaux de zoom **/
		/*
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
		for (int zl = 1; zl <= ZOOM_LEVELS; ++zl) {
			final int zoom_level = zl;
			JavaPairRDD<String, short[][]> finalPairRDD = tmpPairRDD.flatMapToPair((Tuple2<String, short[]> pair) -> {
				List<Tuple2<String, short[][]>> list = new ArrayList<>();
				String location = computeLocation(pair._1.toString());
				int[] coordinates = computeCoordinates(location);
				int y0 = coordinates[0] * INITIAL_SIDE_SIZE;
				int x0 = coordinates[1] * INITIAL_SIDE_SIZE;
				int zoom = (int) Math.pow(2, zoom_level);
				Map<String, short[][]> map = new HashMap<>();
				for (int i = 0; i < pair._2.length; ++i) {
					int posY = y0 + i / INITIAL_SIDE_SIZE;
					int posX = x0 + i % INITIAL_SIDE_SIZE;
					short coordY = (short) (posY % (FINAL_SIDE_SIZE * zoom));
					short coordX = (short) (posX % (FINAL_SIDE_SIZE * zoom));
					short altitude = (short) pair._2[i];
					int keyY = posY / (int) (FINAL_SIDE_SIZE * zoom);
					int keyX = posX / (int) (FINAL_SIDE_SIZE * zoom);
					String key = keyY + "/" + keyX;
					short[][] values = map.get(key);
					if (values != null) {
						values[0] = ArrayUtils.add(values[0], coordY);
						values[0] = ArrayUtils.add(values[0], coordX);
						values[1] = ArrayUtils.add(values[1], altitude);
						map.replace(key, values);
					}
					else {
						values = new short [2][0];
						values[0] = ArrayUtils.add(values[0], coordY);
						values[0] = ArrayUtils.add(values[0], coordX);
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
				int zoom = (int) Math.pow(2, zoom_level);
				BufferedImage bufferedImage = new BufferedImage(FINAL_SIDE_SIZE * zoom, FINAL_SIDE_SIZE * zoom, BufferedImage.TYPE_INT_ARGB);
				for (int i = 0; i < pair._2[1].length; ++i) {
					int y = pair._2[0][2 * i];
					int x = pair._2[0][2 * i + 1];
					int altitude = pair._2[1][i];
					int color = ColorHandler.getColor(altitude);
					bufferedImage.setRGB(x, y, color);
				}
				if (zoom_level > 1) {
					bufferedImage = ImageHandler.resizeImage(bufferedImage, zoom);
				}
				String row = zoom_level + "/" + pair._1;
				byte[][] data = computePictureData(row, bufferedImage);
				HBasePictures.storePictureData(data);
			});
		}
		*/

		context.close();
		System.exit(exitCode);
	}	
}
