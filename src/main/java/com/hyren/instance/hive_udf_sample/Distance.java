package com.hyren.instance.hive_udf_sample;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Distance extends UDF {

	//private static double EARTH_RADIUS = 6378.137;  
	private static double EARTH_RADIUS = 6371.393;

	private static double rad(double d) {

		return d * Math.PI / 180.0;
	}

	/** 
	 * 计算两个经纬度之间的距离 
	 * @param lat1 
	 * @param lng1 
	 * @param lat2 
	 * @param lng2 
	 * @return 
	 */
	public double GetDistance(double lat1, double lng1, double lat2, double lng2) {

		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = s * 1000;
		return s;
	}

	public Double evaluate(Double latitude, Double longitude, Text point) {

		String string = point.toString();
		String[] str = string.substring(6, string.length() - 1).split(" ");

		double disistance = GetDistance(latitude, longitude, Double.valueOf(str[0]), Double.valueOf(str[1]));
		return new Double(disistance);
	}

	public static void main(String[] args) {

		//        System.out.println(MapUtils.GetDistance(29.490295,106.486654,29.615467,106.581515));  
		String string = "POINT(-98.839292 83.928390)";
		DoubleWritable latitude = new DoubleWritable(-98.832299);
		DoubleWritable longitude = new DoubleWritable(83.928390);
		Text text = new Text(string);
		Distance d = new Distance();
		System.out.println(d.evaluate(-98.832299, 81.928390, text));

	}
}
