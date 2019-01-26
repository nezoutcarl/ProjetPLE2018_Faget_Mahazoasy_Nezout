package bigdata;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

		public static Color getGradient(Color color1, Color color2, int percentage) {
			int red = color1.getRed() * percentage + color2.getRed() * (1 - percentage);
			int green = color1.getGreen() * percentage + color2.getGreen() * (1 - percentage);
			int blue = color1.getBlue() * percentage + color2.getBlue() * (1 - percentage);
			return new Color(red, green, blue);
		}

		public static int getColor(int altitude) {
			Color color = null;
			if (altitude <= INTERMEDIATE_ALTITUDE) {
				int range = INTERMEDIATE_ALTITUDE - MINIMUM_ALTITUDE;
				int percentage = (altitude - MINIMUM_ALTITUDE) / range;
				color = getGradient(YELLOW_COLOR, GREEN_WATER_COLOR, percentage);
			}
			else {
				int range = MAXIMUM_ALTITUDE - INTERMEDIATE_ALTITUDE;
				int percentage = (altitude - INTERMEDIATE_ALTITUDE) / range;
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
		String locationX = location.substring(0, 3);
		String locationY = location.substring(3);
		int x = Integer.parseInt(locationX.substring(1));
		int y = Integer.parseInt(locationY.substring(1));
		if (locationX.toLowerCase().charAt(0) == 's') {
			x *= -1;
		}
		if (locationY.toLowerCase().charAt(0) == 'w') {
			y *= -1;
		}
		int[] result = { x, y };
		return result;
	}

	public static byte[][] computePictureData(String path, BufferedImage bufferedImage, int zoom) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(bufferedImage, "png", baos);
		baos.flush();
		byte[] image = baos.toByteArray();
		baos.close();
		String location = computeLocation(path);
		int[] coordinates = computeCoordinates(location);
		byte[][] data = {
				location.getBytes(),
				image,
				{ new Integer(coordinates[0]).byteValue() },
				{ new Integer(coordinates[1]).byteValue() },
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
			int zoom = 0;
			byte[][] data = computePictureData(pair._1.toString(), bufferedImage, zoom);
			HBasePictures.storePictureData(data);
		});*/
		JavaPairRDD<String, Tuple2<int[], int[]>> colorPairRDD = pairRDD.flatMapToPair((Tuple2<Text, IntArrayWritable> pair) -> {
			List<Tuple2<String, Tuple2<int[], int[]>>> list = new ArrayList<Tuple2<String, Tuple2<int[], int[]>>>();
			String location = computeLocation(pair._1.toString());
			int[] coordinates = computeCoordinates(location);
			int y0 = (coordinates[1] + LATITUDE_RANGE) * INITIAL_SIDE_SIZE;
			int x0 = (coordinates[0] + LONGITUDE_RANGE) * INITIAL_SIDE_SIZE;
			for (int i = 0; i < pair._2.getArray().length; ++i) {
				String key = "Y" + (int)(y0 / FINAL_SIDE_SIZE) + "X" + (int)(x0 / FINAL_SIDE_SIZE);
				int[] coord = { i };
				int[] value = { ColorHandler.getColor(pair._2.getArray()[i])};
				list.add(new Tuple2<String, Tuple2<int[], int[]>>(key, new Tuple2<int[], int[]>(coord, value)));
			}
			return list.iterator();
		});
		JavaPairRDD<String, Tuple2<int[], int[]>> finalColorPairRDD = colorPairRDD.reduceByKey((Tuple2<int[], int[]> pair1, Tuple2<int[], int[]> pair2) -> {
			int[] coords = ArrayUtils.addAll(pair1._1, pair2._1);
			int[] values = ArrayUtils.addAll(pair1._2, pair2._2);
			return new Tuple2<int[], int[]>(coords, values);
		});
		finalColorPairRDD.foreach((Tuple2<String, Tuple2<int[], int[]>> pair) -> {
			BufferedImage bufferedImage = new BufferedImage(FINAL_SIDE_SIZE, FINAL_SIDE_SIZE, BufferedImage.TYPE_INT_ARGB);
			for (int i = 0; i < pair._2._1.length; ++i) {
				bufferedImage.setRGB(pair._2._1[i] % FINAL_SIDE_SIZE, pair._2._1[i] / FINAL_SIDE_SIZE, new Color(pair._2._2[i]).getRGB());
			}
			int zoom = 0;
			byte[][] data = computePictureData(pair._1, bufferedImage, zoom);
			HBasePictures.storePictureData(data);
		});
		context.close();
		System.exit(exitCode);
	}
}
