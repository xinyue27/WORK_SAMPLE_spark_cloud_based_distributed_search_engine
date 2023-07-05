package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	private String inputTableName;
	private KVSClient kvsClient;
	private FlameContextImpl flameContextImpl;
	
	public FlamePairRDDImpl(KVSClient kvsClient, FlameContextImpl flameContextImpl) {
		this.kvsClient = kvsClient;
		this.flameContextImpl = flameContextImpl;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
//		System.out.println("FlamePairRDDImpl collect() invoked.");
//		System.out.println("FlamePairRDDImpl tablename: " + this.inputTableName);
		List<FlamePair> res = new ArrayList<FlamePair>();
		if (this.inputTableName == null || this.kvsClient == null) {
			// directly return empty list;
			return res;
		}
		try {
			Iterator<Row> rows = this.kvsClient.scan(this.inputTableName, null, null);
			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
				for (String c: columns) {
					byte[] value = this.kvsClient.get(this.inputTableName, rowKey, c);
//					System.out.println("FlamePairRDDImpl collect() value: " + new String(value));
					res.add(new FlamePair(rowKey, new String(value)));
				}
			}
		} catch (Exception e){
			// if anything happens, do not add to result
		}
		return res;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String operation = "/pairRdd/foldByKey";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object pairRdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, zeroElement);
		return (FlamePairRDD)pairRdd;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		if (!this.inputTableName.equals(tableNameArg)) {
//			System.out.println("renaming " + this.inputTableName + " to new name: " + tableNameArg);
			this.kvsClient.rename(this.inputTableName, tableNameArg);
			this.inputTableName = tableNameArg;
		}

	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
//		System.out.println("FlamePairRDDImpl flatMap() called.");
		String operation = "/pairRdd/flatMap";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlameRDD)rdd;
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
//		System.out.println("FlamePairRDDImpl flatMapToPair() called.");
		String operation = "/pairRdd/flatMapToPair";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = flameContextImpl.invokeOperation(operation, lambdaSerialized, this.inputTableName, null, null);
		return (FlamePairRDD)rdd;
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
//		System.out.println("FlamePairRDDImpl join() called.");
		String operation = "/pairRdd/join";
		String inputTableName2 = ((FlamePairRDDImpl)other).getInputTable();
		Object rdd = flameContextImpl.invokeOperation(operation, null, this.inputTableName, inputTableName2, null);
		return (FlamePairRDD)rdd;
	}

	public void setInputTable(String tableName) {
		this.inputTableName = tableName;
	}
	
	public String getInputTable() {
		return this.inputTableName;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
//		System.out.println("FlamePairRDDImpl cogroup() called.");
		String operation = "/pairRdd/cogroup";
		String inputTableName2 = ((FlamePairRDDImpl)other).getInputTable();
		Object rdd = flameContextImpl.invokeOperation(operation, null, this.inputTableName, inputTableName2, null);
		return (FlamePairRDD)rdd;
	}
}
