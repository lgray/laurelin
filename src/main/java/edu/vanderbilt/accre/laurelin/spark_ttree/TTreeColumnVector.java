package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.util.List;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;

public class TTreeColumnVector extends ColumnVector {
	List<TBasket> baskets;
	
	protected TTreeColumnVector(DataType type, List<TBasket> baskets) {
		super(type);
		this.baskets = baskets;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean hasNull() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int numNulls() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isNullAt(int rowId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBoolean(int rowId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte(int rowId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(int rowId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(int rowId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(int rowId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(int rowId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(int rowId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ColumnarArray getArray(int rowId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnarMap getMap(int ordinal) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Decimal getDecimal(int rowId, int precision, int scale) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UTF8String getUTF8String(int rowId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBinary(int rowId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ColumnVector getChild(int ordinal) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/*
	 * Return 
	 */

	@Override
	public boolean[] getBooleans(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getBooleans(rowId, count);
	}

	@Override
	public byte[] getBytes(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getBytes(rowId, count);
	}

	@Override
	public short[] getShorts(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getShorts(rowId, count);
	}

	@Override
	public int[] getInts(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getInts(rowId, count);
	}

	@Override
	public long[] getLongs(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getLongs(rowId, count);
	}

	@Override
	public float[] getFloats(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getFloats(rowId, count);
	}

	@Override
	public double[] getDoubles(int rowId, int count) {
		// TODO Auto-generated method stub
		return super.getDoubles(rowId, count);
	}	
}