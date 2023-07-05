package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;

import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.generic.Worker;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class FlameWorker extends Worker {
	private final File myJAR;

	public FlameWorker(File myJAR) {
		this.myJAR = myJAR;
//		registerFlameWorkerRoutes();

	}

	private void registerFlameWorkerRoutes() {
		postUserJAR();
		postRddFlatMap();
		postRddMapToPair();
		postRddFoldByKey();
		postRddGroupBy();
		postRddFromTable();
		postPairRddFlatMap();
		postPairRddFlatMapToPair();
		postRddFlatMapToPair();
		postRddDistinct();
		postPairRddJoin();
		postRddFold();
		// ec
		postRddFilter();
		postRddMapPartitions();
		postPairRddCoGroup();
	}



	private void postUserJAR() {
		post("/useJAR", (request, response) -> {
			FileOutputStream fos = new FileOutputStream(myJAR);
			fos.write(request.bodyAsBytes());
			fos.close();
			return "OK";
		});

	}

	private void postRddFlatMap() {
		post("/rdd/flatMap", (request, response) -> {

//		    	System.out.println("/rdd/flatMap router called at: " + System.currentTimeMillis());

			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			byte[] lambdaSerialized = request.bodyAsBytes();
//			System.out.println("labmdaSerialized: " + lambdaSerialized);
//			for (byte b: lambdaSerialized) {
//				System.out.print(b + " ");
//			}
//			System.out.println();

//			System.out.println("myJAR: " + myJAR);
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, myJAR);

//			System.out.println("lambdaDeserialized success: " + lambdaDeserialized);
			KVSClient kvsClient = new KVSClient(kvsMaster);

			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);

			if (rows == null) {
				return "OK";
			}

			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
//					Set<String> columns = row.columns();
//					System.out.println("columns: " + columns);
//					for (String c: columns) {
				byte[] value = kvsClient.get(inputTableName, rowKey, "value");
//						System.out.println("getting value: " + new String(value));
				Iterable<String> res = ((StringToIterable) lambdaDeserialized).op(new String(value));
				for (String s : res) {
//							System.out.println("res s: " + s);
					String newRowKey = UUID.randomUUID().toString();
					kvsClient.put(outputTableName, newRowKey, "value", s.getBytes());
//							System.out.println("putting " + s + " to table " + outputTableName + " with rowKey: " + newRowKey + "ts: " + System.currentTimeMillis());
//						}
				}

			}

			return "OK";
		});
	}

	private void postRddMapToPair() {
		post("/rdd/mapToPair", (request, response) -> {
//			System.out.println("/rdd/mapToPair router called.");

			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, myJAR);

			KVSClient kvsClient = new KVSClient(kvsMaster);

			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
//	    	System.out.println("in FlameWorker, kvsClient has numWorkers=" + kvsClient.numWorkers());

			if (rows == null) {
				return "OK";
			}

			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
//				Set<String> columns = row.columns();
//				System.out.println("columns: " + columns);
//				for (String c: columns) {
				byte[] value = kvsClient.get(inputTableName, rowKey, "value");

//				System.out.println("/rdd/mapToPair getting value: " + new String(value));

				FlamePair flamePair = ((StringToPair) lambdaDeserialized).op(new String(value));
				String k = flamePair._1();
				String v = flamePair._2();
//				System.out.println("flamepair string 1: " + flamePair._1());
//				System.out.println("flamepair string 2: " + flamePair._2());

//				kvsClient.put(outputTableName, k, rowKey, v.getBytes());
				String columnName = UUID.randomUUID().toString();
//				System.out.println("putting " + v + " to table " + outputTableName + " to row: " + k + " with column key: " + columnName);			
				kvsClient.put(outputTableName, k, columnName, v.getBytes());

//				}

			}
			return "OK";
		});

	}

	private void postRddFlatMapToPair() {
		post("/rdd/flatMapToPair", (request, response) -> {

//	    System.out.println("/rdd/flatMapToPair router called at: " + System.currentTimeMillis());

		Map<String, String> queryParams = parseQueryParams(request);
		String inputTableName = queryParams.get("inputTableName1");
		String outputTableName = queryParams.get("outputTableName");
		String kvsMaster = queryParams.get("kvsMaster");
		String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
		String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
				: queryParams.get("toKeyExclusive");

		byte[] lambdaSerialized = request.bodyAsBytes();
		Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, myJAR);

		KVSClient kvsClient = new KVSClient(kvsMaster);

		Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);

		if (rows == null) {
			return "OK";
		}

		while (rows.hasNext()) {
			Row row = rows.next();
			String rowKey = row.key();
//				Set<String> columns = row.columns();
//				System.out.println("columns: " + columns);
//				for (String c: columns) {
			byte[] value = kvsClient.get(inputTableName, rowKey, "value");
//			System.out.println("/rdd/flatMapToPair getting value: " + new String(value));
			Iterable<FlamePair> res = ((StringToPairIterable) lambdaDeserialized).op(new String(value));
			for (FlamePair flamePair : res) {
				String k = flamePair._1();
				String v = flamePair._2();
//						System.out.println("res s: " + s);
//				String newRowKey = UUID.randomUUID().toString();
				kvsClient.put(outputTableName, k, "value", v.getBytes());
//						System.out.println("putting " + s + " to table " + outputTableName + " with rowKey: " + newRowKey + "ts: " + System.currentTimeMillis());
//					}
			}

		}

		return "OK";
	});
	}
	

	private void postRddFoldByKey() {
		post("/pairRdd/foldByKey", (request, response) -> {
//	    	System.out.println("/rdd/foldByKey router called.");

			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");
			String zeroElement = queryParams.get("zeroElement");

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, myJAR);

			KVSClient kvsClient = new KVSClient(kvsMaster);

			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
//	    	System.out.println("in FlameWorker, kvsClient has numWorkers=" + kvsClient.numWorkers());

			if (rows == null) {
				return "OK";
			}

			while (rows.hasNext()) {
//	    		int accumulator = Integer.parseInt(zeroElement);
				String accumulator = zeroElement;
				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
//				System.out.println("columns: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName, rowKey, c);
//					System.out.println("getting value: " + new String(value));
					String result = ((TwoStringsToString) lambdaDeserialized).op(new String(value), accumulator);
//					System.out.println("accumulator: " + accumulator);
					accumulator = result;
				}
				String columnKey = UUID.randomUUID().toString();
				kvsClient.put(outputTableName, rowKey, columnKey, accumulator.getBytes());

			}

			return "OK";
		});

	}

	private void postRddGroupBy() {

		post("/rdd/groupBy", (request, response) -> {
//	    	System.out.println("/rdd/groupBy router called.");

			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, myJAR);

			KVSClient kvsClient = new KVSClient(kvsMaster);

			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}
			while (rows.hasNext()) {
//	    		int accumulator = Integer.parseInt(zeroElement);

				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
//				System.out.println("columns: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName, rowKey, c);
//					System.out.println("getting value: " + new String(value) + " with rowKey=" + rowKey + " columnName=" + c);
					String result = ((StringToString) lambdaDeserialized).op(new String(value));
//					System.out.println("result: " + result);

//					String columnKey = UUID.randomUUID().toString();
					String previousVal = new String(kvsClient.get(outputTableName, result, c));
//					System.out.println("previously stored: " + previousVal);
					String newVal = "";
					if (previousVal.contains("not found")) {
						previousVal = "";
						newVal = new String(value);
					} else {
						newVal = previousVal + "," + new String(value);
					}

					kvsClient.put(outputTableName, result, c, newVal.getBytes());
//					System.out.println("putting value: " + newVal + " to table: " + outputTableName + " with rowKey=" + result + " columnName=" + c);
				}

			}

			return "OK";
		});

	}

	private void postRddFromTable() {
		post("/rdd/fromTable", (request, response) -> {
//			System.out.println("FlameWorker /rdd/fromTable invoked.");
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, this.myJAR);

			while (rows.hasNext()) {
				Row row = rows.next();
				String result = ((RowToString) lambdaDeserialized).op(row);
//				System.out.println("/rdd/fromTable lambda op result: " + result);
				if (result != null) {
					String newRowKey = UUID.randomUUID().toString();
					kvsClient.put(outputTableName, newRowKey, "value", result.getBytes());
				}
			}
			return "OK";
		});

	}

	private void postPairRddFlatMap() {
		post("/pairRdd/flatMap", (request, response) -> {
//			System.out.println("FlameWorker /pairRdd/flatMap invoked.");
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, this.myJAR);

			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
//				System.out.println("/pairRDD/flatMap rowKey: " + rowKey);
				Set<String> columns = row.columns();
				for (String c : columns) {
//					System.out.println("/pairRDD/flatMap column name: " + c);
					byte[] value = kvsClient.get(inputTableName, rowKey, c);
//					System.out.println("/pairRDD/flatMap getting value: " + new String(value));
//		    		FlamePair pair = ((StringToPair)lambdaDeserialized).op(new String(value));
//		    		System.out.println("transform value to pair: " + pair);
					FlamePair pair = new FlamePair(rowKey, new String(value));
					Iterable<String> result = ((PairToStringIterable) lambdaDeserialized).op(pair);
					String newRowKey = UUID.randomUUID().toString();
					for (String s : result) {
//						System.out.println("/pairRDD/flatMap result s: " + s);
						String newColumnName = UUID.randomUUID().toString();
						kvsClient.put(outputTableName, newRowKey, newColumnName, s.getBytes());
//						
					}
				}
			}

			return "OK";
		});
	};

	private void postPairRddFlatMapToPair() {
		post("/pairRdd/flatMapToPair", (request, response) -> {
//			System.out.println("FlameWorker /pairRdd/flatMapToPair invoked.");
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, this.myJAR);

			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
//				System.out.println("/pairRdd/flatMapToPair rowKey: " + rowKey);
				Set<String> columns = row.columns();
				for (String c : columns) {
//					System.out.println("/pairRdd/flatMapToPair column name: " + c);
					byte[] value = kvsClient.get(inputTableName, rowKey, c);
//					System.out.println("/pairRDD/flatMapToPair value: " + new String(value));
					FlamePair pair = new FlamePair(rowKey, new String(value));
					Iterable<FlamePair> result = ((PairToPairIterable) lambdaDeserialized).op(pair);
//		    		String newRowKey = UUID.randomUUID().toString();
					for (FlamePair fp : result) {
//		    			String newColumnName = UUID.randomUUID().toString();
						String k = fp._1();
						String v = fp._2();
//						System.out.println("/pairRdd/flatMapToPair fp 1: " + k);
//						System.out.println("/pairRdd/flatMapToPair fp 2: " + v);
						kvsClient.put(outputTableName, k, rowKey, v.getBytes());
					}
				}
			}

			return "OK";
		});

	}

	private void postRddDistinct() {
		post("/rdd/distinct", (request, response) -> {
//			System.out.println("FlameWorker /rdd/distinct invoked.");
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}
			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
//				System.out.println("/rdd/distinct rowKey: " + rowKey);
				Set<String> columns = row.columns();
				for (String c : columns) {

//					System.out.println("/rdd/distinct column name: " + c);
					byte[] value = kvsClient.get(inputTableName, rowKey, c);
//					System.out.println("/rdd/distinct getting value: " + new String(value));
					kvsClient.put(outputTableName, new String(value), "value", value);
				}
			}
			return "OK";
		});
	}

	private void postPairRddJoin() {
		post("/pairRdd/join", (request, response) -> {
//			System.out.println("FlameWorker /pairRdd/join invoked.");

			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName1 = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");

			String inputTableName2 = queryParams.get("inputTableName2");
			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows1 = kvsClient.scan(inputTableName1, fromKey, toKeyExclusive);
			Iterator<Row> rows2 = kvsClient.scan(inputTableName2, fromKey, toKeyExclusive);

			// if either one is null, there will be no element in join
			if (rows1 == null || rows2 == null) {
				return "OK";
			}
			Map<String, Row> rows2Cache = new HashMap<String, Row>();
			while (rows2.hasNext()) {
				Row row2 = rows2.next();
				rows2Cache.put(row2.key(), row2);

			}
			while (rows1.hasNext()) {
				Row row1 = rows1.next();
				String rowKey1 = row1.key();
//				System.out.println("/pairRdd/join rowKey1: " + rowKey1);
				if (rows2Cache.containsKey(rowKey1)) {
					// element exists in both tables
					Row row2 = rows2Cache.get(rowKey1);
					Set<String> columns1 = row1.columns();
					Set<String> columns2 = row2.columns();
					for (String c1 : columns1) {
						byte[] value1 = kvsClient.get(inputTableName1, rowKey1, c1);
						for (String c2 : columns2) {
							byte[] value2 = kvsClient.get(inputTableName2, rowKey1, c2);
							String newVal = new String(value1) + "," + new String(value2);
							kvsClient.put(outputTableName, rowKey1, UUID.randomUUID().toString(), newVal);
//	    					if (new String(value1).equals(new String(value2))) {
//	    						System.out.println(new String(value1) + " exists in both tables.");
//	    						kvsClient.put(outputTableName, UUID.randomUUID().toString(), UUID.randomUUID().toString(), value1);
//	    					}
						}
					}
				}

			}
			return "OK";
		});
	}

	private void postRddFold() {
		post("/rdd/fold", (request, response) -> {
//			System.out.println("FlameWorker /rdd/fold invoked.");

			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName1 = queryParams.get("inputTableName1");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");
			String zeroElement = queryParams.get("zeroElement");
//			System.out.println("fromKey: " + fromKey + " toKeyExclusive: " + toKeyExclusive);
			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName1, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, this.myJAR);

			String accumulator = zeroElement;
			while (rows.hasNext()) {

				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
//				System.out.println("/rdd/fold columns: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName1, rowKey, c);
//					System.out.println("/rdd/fold getting value: " + new String(value));
					String result = ((TwoStringsToString) lambdaDeserialized).op(new String(value), accumulator);
//					System.out.println("/rdd/fold result: " + result);
					accumulator = result;
				}
//				System.out.println("row accumulator result: " + accumulator);
			}
//	    	response.body(accumulator);
//			return "OK";
			return accumulator;
		});
	}
	
	private void postRddFilter() {
		// TODO Auto-generated method stub
		post("/rdd/filter", (request, response) -> {
//			System.out.println("FlameWorker /rdd/filter invoked.");
			
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName1 = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");
			
			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName1, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, this.myJAR);

			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
//				System.out.println("/rdd/fold columns: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName1, rowKey, c);
//					System.out.println("/rdd/fold getting value: " + new String(value));
					boolean result = ((StringToBoolean) lambdaDeserialized).op(new String(value));
//					System.out.println("/rdd/fold result: " + result);
					if (result) {
						kvsClient.put(outputTableName, UUID.randomUUID().toString(), "value", value);
					}
				}
			}
			return "OK";
		});
	}
	

	private void postRddMapPartitions() {
		post("/rdd/mappartitions", (request, response) -> {
//			System.out.println("FlameWorker /rdd/mappartitions invoked.");
			
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName1 = queryParams.get("inputTableName1");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");
			
			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows = kvsClient.scan(inputTableName1, fromKey, toKeyExclusive);
			if (rows == null) {
				return "OK";
			}

			byte[] lambdaSerialized = request.bodyAsBytes();
			Object lambdaDeserialized = Serializer.byteArrayToObject(lambdaSerialized, this.myJAR);

			List<String> input = new ArrayList<String>();
			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
//				System.out.println("/rdd/fold columns: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName1, rowKey, c);
//					System.out.println("/rdd/fold getting value: " + new String(value));
					input.add(new String(value));
				}
			}
			Iterator<String> inputIter = input.iterator();
			Iterator<String> result = ((IteratorToIterator)lambdaDeserialized).op(inputIter);
			while (result.hasNext()) {
				String s = result.next();
//				System.out.println("result s: " + s);
				kvsClient.put(outputTableName, UUID.randomUUID().toString(), "value", s.getBytes());
			}
			return "OK";
		});
	}
	

	private void postPairRddCoGroup() {
		post("/pairRdd/cogroup", (request, response) -> {
//			System.out.println("FlameWorker /pairRdd/cogroup invoked.");
			
			Map<String, String> queryParams = parseQueryParams(request);
			String inputTableName1 = queryParams.get("inputTableName1");
			String inputTableName2 = queryParams.get("inputTableName2");
			String outputTableName = queryParams.get("outputTableName");
			String kvsMaster = queryParams.get("kvsMaster");
			String fromKey = queryParams.get("fromKey").equals("null") ? null : queryParams.get("fromKey");
			String toKeyExclusive = queryParams.get("toKeyExclusive").equals("null") ? null
					: queryParams.get("toKeyExclusive");
			
			KVSClient kvsClient = new KVSClient(kvsMaster);
			Iterator<Row> rows1 = kvsClient.scan(inputTableName1, fromKey, toKeyExclusive);
			Iterator<Row> rows2 = kvsClient.scan(inputTableName2, fromKey, toKeyExclusive);
			if (rows1 == null) {
				return "OK";
			}

//			Map<String, List<String>> cache1 = new HashMap<String, List<String>>();
			Map<String, Map<String, List<String>>> cache = new HashMap<String, Map<String, List<String>>>();
			while (rows1.hasNext()) {
				Row row = rows1.next();
				String rowKey = row.key();
//				System.out.println("/pairRdd/cogroup rowkey1: " + rowKey);
				if (!cache.containsKey(rowKey)) {
//					cache1.put(rowKey, new ArrayList<String>());
					cache.put(rowKey, new HashMap<String, List<String>>());
					cache.get(rowKey).put("table1", new ArrayList<String>());
					cache.get(rowKey).put("table2", new ArrayList<String>());
				}
				Set<String> columns = row.columns();
//				System.out.println("/pairRdd/cogroup columns1: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName1, rowKey, c);
//					System.out.println("/pairRdd/cogroup getting value1: " + new String(value));
//					cache1.get(rowKey).add(new String(value));
					cache.get(rowKey).get("table1").add(new String(value));
				}
			}
//			Map<String, List<String>> cache2 = new HashMap<String, List<String>>();
			while (rows2.hasNext()) {
				Row row = rows2.next();
				String rowKey = row.key();
//				System.out.println("/pairRdd/cogroup rowkey2: " + rowKey);
				if (!cache.containsKey(rowKey)) {
//					cache2.put(rowKey, new ArrayList<String>());
					cache.put(rowKey, new HashMap<String, List<String>>());
					cache.get(rowKey).put("table1", new ArrayList<String>());
					cache.get(rowKey).put("table2", new ArrayList<String>());
				}
				Set<String> columns = row.columns();
//				System.out.println("/pairRdd/cogroup columns2: " + columns);
				for (String c : columns) {
					byte[] value = kvsClient.get(inputTableName2, rowKey, c);
//					System.out.println("/pairRdd/cogroup getting value2: " + new String(value));
//					cache2.get(rowKey).add(new String(value));
					cache.get(rowKey).get("table2").add(new String(value));
				}
			}
//			System.out.println("cache: " + cache);
			for (String k: cache.keySet()) {
				List<String> table1 = cache.get(k).get("table1");
				List<String> table2 = cache.get(k).get("table2");
				StringBuilder sb = new StringBuilder();
				String prefix = "";
				sb.append("[");
				for (String s: table1) {
					sb.append(prefix).append(s);
					prefix = ",";
				}
				sb.append("],[");
				prefix = "";
				for (String s: table2) {
					sb.append(prefix).append(s);
					prefix = ",";
				}
				sb.append("]");
				String value = sb.toString();
//				System.out.println("new value: " + value);
				kvsClient.put(outputTableName, k, UUID.randomUUID().toString(), value);
			}
			
			return "OK";
		});

	}

	private Map<String, String> parseQueryParams(Request request) throws UnsupportedEncodingException {
		String optionalInputTableName1 = request.queryParams("inputTableName1");
		String inputTableName1 = null;
		if (optionalInputTableName1 != null) {
			inputTableName1 = URLDecoder.decode(request.queryParams("inputTableName1"), "UTF-8");
		}

		String optionalInputTableName2 = request.queryParams("inputTableName2");
		String inputTableName2 = null;
		if (optionalInputTableName2 != null) {
			inputTableName2 = URLDecoder.decode(request.queryParams("inputTableName2"), "UTF-8");
		}

		String outputTableName = URLDecoder.decode(request.queryParams("outputTableName"), "UTF-8");
		String kvsMaster = URLDecoder.decode(request.queryParams("kvsMaster"), "UTF-8");
		String fromKey = URLDecoder.decode(request.queryParams("fromKey"), "UTF-8");
		String toKeyExclusive = URLDecoder.decode(request.queryParams("toKeyExclusive"), "UTF-8");

		String optionalZeroElement = request.queryParams("zeroElement");
		String zeroElement = null;
		if (optionalZeroElement != null) {
			zeroElement = URLDecoder.decode(request.queryParams("zeroElement"), "UTF-8");
			;
		}

//    	String zeroElement = URLDecoder.decode(request.queryParams("zeroElement"), "UTF-8");
//    	String inputTableName2 = URLDecoder.decode(request.queryParams("inputTableName2"), "UTF-8");
		Map<String, String> map = new HashMap<String, String>();
		map.put("inputTableName1", inputTableName1);
		map.put("outputTableName", outputTableName);
		map.put("kvsMaster", kvsMaster);
		map.put("fromKey", fromKey);
		map.put("toKeyExclusive", toKeyExclusive);
		map.put("inputTableName2", inputTableName2);
		map.put("zeroElement", zeroElement);
//		System.out.println("qparam map: " + map);
		return map;
	}

	public static void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Syntax: FlameWorker <port> <masterIP:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		String server = args[1];
		startPingThread(server, "" + port, port);
		final File myJAR = new File("__worker" + port + "-current.jar");

		port(port);

		FlameWorker flameWorker = new FlameWorker(myJAR);
		flameWorker.registerFlameWorkerRoutes();

	}
}
