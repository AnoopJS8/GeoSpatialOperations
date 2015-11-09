package edu.asu.cse512;

import java.io.Serializable;
import java.lang.Math;

public class GeoPoint implements Comparable<GeoPoint>, Serializable {
	private static final long serialVersionUID = 4106187863140078576L;
	private final double x;
	private final double y;
	private int id;

	public GeoPoint(double x, double y) {
		this.x = x;
		this.y = y;
	}
	public GeoPoint(int id, double x, double y) {
		this.x = x;
		this.y = y;
		this.id = id;
	}
	
	public double getX() {
		return x;
	}
	public double getY() {
		return y;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public GeoPoint(String str){
		String splitStr[] = str.split(","); 
		this.x = Double.parseDouble(splitStr[0]);
		this.y = Double.parseDouble(splitStr[1]);
	}

	public double getDistance(GeoPoint p){
		return Math.pow(x - p.x, 2) + Math.pow(y - p.y, 2);
	}
	
	public static double getDistance(GeoPoint p, GeoPoint q){
		return Math.pow(p.x - q.x, 2) + Math.pow(p.y - q.y, 2);
	}

	public double getSlope(GeoPoint toPoint)
	{
		return (toPoint.y() - this.y()) / (toPoint.x() - this.x());
	}

	public double x() {
		return this.x;
	}

	public double y() {
		return this.y;
	}

	public String toString()
	{
		return new String(x + "," + y);
	}
	
	public int compareTo(GeoPoint p) {
		if (this.x == p.x) {
			return (int) (this.y - p.y);
		} else {
			return (int)(this.x - p.x);
		}
	} 

	@Override 
	public boolean equals(Object compareTo) {
		if (compareTo instanceof GeoPoint) {
			GeoPoint compare = (GeoPoint) compareTo;
			return ((this.x() == compare.x()) && (this.y() == compare.y()));
		}
		return false;
	}

	@Override 
	public int hashCode() {
		return (int) (41.0 * ((41000 * this.x()) + (1000.0 * this.y()))/1000.0);
	}
};


