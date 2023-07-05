package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	private String inputTableName;
	private KVSClient kvsClient;
	private FlameContextImpl flameContextImpl;
	
	public FlameRDDImpl() {
		
	}
	public FlameRDDImpl(KVSClient kvsClient, FlameContextImpl flameContextImpl) {
		this.kvsClient = kvsClient;
		this.flameContextImpl = flameContextImpl;
	}
	
	@Override
	public int count() throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("FlameRDDImpl count(): " + this.kvsClient.count(this.inputTableName));
		return this.kvsClient.count(this.inputTableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		if (!this.inputTableName.equals(tableNameArg)) {
//			System.out.println("renaming " + this.inputTableName + " to new name: " + tableNameArg);
			this.kvsClient.rename(this.inputTableName, tableNameArg);
			this.inputTableName = tableNameArg;
		}
	}

	@Override
	public FlameRDD distinct() throws Exception {
//		System.out.println("FlameRDDImpl distinct() called.");
		String operation = "/rdd/distinct";
//		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, null, this.inputTableName, null, null);
		return (FlameRDD)rdd;
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		// TODO Auto-generated method stub
		Vector<String> res = new Vector<String>();
		Iterator<Row> rows = this.kvsClient.scan(this.inputTableName, null, null);
		int i = 0;
		while (rows.hasNext() && i < num) {
			Row row = rows.next();
			String rowKey = row.key();
			byte[] value = this.kvsClient.get(this.inputTableName, rowKey, "value");
			res.add(new String(value));
			i++;
		}
//		System.out.println("FlameRDDImpl take() result: " + res);
		return res;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		String operation = "/rdd/fold";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object res = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, zeroElement);
		return (String)res;
	}

	@Override
	public List<String> collect() throws Exception {
//		System.out.println("FlameRDDImpl collect() invoked.");
		List<String> res = new ArrayList<String>();
		if (this.inputTableName == null || this.kvsClient == null) {
			// directly return empty list;
//			System.out.println("inputTableName is null || kvsClient is null.");
			return res;
		}
		System.out.println("FlameRDDImpl collect() input table name: " + this.inputTableName);
		// inputTableName is set by FlameContextImpl parallelize().
		Iterator<Row> rows = this.kvsClient.scan(this.inputTableName, null, null);
		while (rows.hasNext()) {
			Row row = rows.next();
//			System.out.println("row: " + row);
			String rowKey = row.key();
//			System.out.println("rowKey: " + rowKey);
			Set<String> columns = row.columns();
//			System.out.println("columns: " + columns);
			for (String c: columns) {
				byte[] value = this.kvsClient.get(this.inputTableName, rowKey, c);
//				System.out.println("FlameRDDImpl collect() value: " + new String(value));
				res.add(new String(value));
			}
		}
		return res;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
//		System.out.println("FlameRDDImpl flatMap() called.");
		String operation = "/rdd/flatMap";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlameRDD)rdd;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
//		System.out.println("FlameRDDImpl flatMapToPair() called.");
		String operation = "/rdd/flatMapToPair";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlamePairRDD)rdd;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
//		System.out.println("FlameRDDImpl mapToPair() called.");
		String operation = "/rdd/mapToPair";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object pairRdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlamePairRDD)pairRdd;
	}

	@Override
	public FlameRDD intersection(FlameRDD other) throws Exception {
		FlameRDD rdd = flameContextImpl.invokeIntersection(this, (FlameRDDImpl)other);
		return rdd;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		String operation = "/rdd/groupBy";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object pairRdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlamePairRDD)pairRdd;
	}

	public void setInputTable(String tableName) {
		this.inputTableName = tableName;
	}
	
	public String getInputTable() {
		return this.inputTableName;
	}
	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		String operation = "/rdd/filter";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlameRDD)rdd;
	}
	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		String operation = "/rdd/mappartitions";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlameRDD)rdd;
	}

}
