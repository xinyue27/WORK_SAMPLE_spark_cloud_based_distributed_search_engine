package cis5550.flame;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.HTTP;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext {
	private StringBuilder sb;
	private KVSClient kvsClient;

	public FlameContextImpl(String jarName, KVSClient kvsClient) {
		this.sb = new StringBuilder();
		this.kvsClient = kvsClient;

	}

	@Override
	public void output(String s) {
		sb.append(s);
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
//		System.out.println("FlameContextImpl parallelize()");
		String tableName = UUID.randomUUID().toString();
//		System.out.println("FlameContextImpl parallelize() table name: " + tableName);
		for (String s : list) {
//			System.out.println("FlameContextImpl parallelize() processing s: " + s);

			String rowKey = UUID.randomUUID().toString();
//			System.out.println("putting " + s + " into table " + tableName);
			this.kvsClient.put(tableName, rowKey, "value", s.getBytes());
			
		}

		FlameRDDImpl flameRDDImpl = new FlameRDDImpl(this.kvsClient, this);
		flameRDDImpl.setInputTable(tableName);
//		System.out.println("FlameContextImpl parallelize() setting flameRDDImpl tableName: " + tableName);
		FlamePairRDDImpl flamePairRDDImpl = new FlamePairRDDImpl(this.kvsClient, this);
		flamePairRDDImpl.setInputTable(tableName);
//		System.out.println("FlameContextImpl parallelize() setting flamePairRDDImpl tableName: " + tableName);
		return flameRDDImpl;
	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		String operation = "/rdd/fromTable";
		byte[] lambdaSerialized = Serializer.objectToByteArray(lambda);
		Object rdd = invokeOperation(operation, lambdaSerialized, tableName, null, null);
		return (FlameRDD) rdd;
	}

	// this function is called by mapToPair(), foldByKey() etc.
	// next time look into potentially get rid of this inputTable parameter.
	public Object invokeOperation(String operationName, byte[] lambda, String inputTableName1, String inputTableName2,
			String zeroElement) {
//		System.out.println("FlameContextImpl invokeOperation operationName: " + operationName + " inputTableName1: "
//				+ inputTableName1);
//		System.out.println("project before sending lambda: " + lambda);
//		for (byte b: lambda) {
//			System.out.print(b + " ");
//		}
//		System.out.println();
		// generate a fresh output table to store results after this operation.
		String outputTableName = UUID.randomUUID().toString();
//		System.out.println("invokeOperation generating outputTableName: " + outputTableName);
		Vector<String> flameWorkers = FlameMaster.getWorkers();

		try {
			int numKvsWorkers = kvsClient.numWorkers();
//			System.out.println("numKvsWorkers: " + numKvsWorkers);
			Partitioner partitioner = new Partitioner();

			for (int i = 0; i < numKvsWorkers - 1; i++) {
				String kvsWorkerAddress = kvsClient.getWorkerAddress(i);
				String kvsWorkerId = kvsClient.getWorkerID(i);
				String nextKvsWorkerId = kvsClient.getWorkerID(i + 1);
//					System.out.println("kvsWorkerId: " + kvsWorkerId + " nextKvsWorkerId: " + nextKvsWorkerId + " kvsWorkerAddress: " + kvsWorkerAddress);
				partitioner.addKVSWorker(kvsWorkerAddress, kvsWorkerId, nextKvsWorkerId);
			}
			String lowestKvsWorkerId = kvsClient.getWorkerID(0);
			String highestKvsWorkerId = kvsClient.getWorkerID(numKvsWorkers - 1);
			String highestKvsWorkerAddress = kvsClient.getWorkerAddress(numKvsWorkers - 1);

			partitioner.addKVSWorker(highestKvsWorkerAddress, null, lowestKvsWorkerId);
			partitioner.addKVSWorker(highestKvsWorkerAddress, highestKvsWorkerId, null);

//			}
			for (String workerId : flameWorkers) {
//				System.out.println("flameworker id: " + workerId);
				partitioner.addFlameWorker(workerId);
			}

			Vector<Partition> partitions = partitioner.assignPartitions();

			int n = partitions.size();
//			System.out.println("partitions size: " + n);
			Thread threads[] = new Thread[n];
			String results[] = new String[n];
			Response responses[] = new Response[n];
			int resultsStatusCodes[] = new int[n];

			for (int i = 0; i < n; i++) {
				Partition p = partitions.elementAt(i);
//				System.out.println("partition kvsWorker: " + p.kvsWorker);
//				System.out.println("partition fromKey: " + p.fromKey);
//				System.out.println("partition toKeyExclusive: " + p.toKeyExclusive);
//				System.out.println("paritition assignedFlameWorker: " + p.assignedFlameWorker);

				StringBuilder sb = new StringBuilder();
				sb.append("http://");
				sb.append(p.assignedFlameWorker);
				sb.append(operationName);
				
				sb.append("?inputTableName1=").append(URLEncoder.encode(inputTableName1, "UTF-8"));

				if (inputTableName2 != null) {
					sb.append("&inputTableName2=").append(URLEncoder.encode(inputTableName2, "UTF-8"));
				}
				sb.append("&outputTableName=").append(URLEncoder.encode(outputTableName, "UTF-8"));
				sb.append("&kvsMaster=").append(URLEncoder.encode(this.kvsClient.getMaster(), "UTF-8"));

				sb.append("&fromKey=").append(URLEncoder.encode(p.fromKey == null ? "null" : p.fromKey, "UTF-8"));
				sb.append("&toKeyExclusive=")
						.append(URLEncoder.encode(p.toKeyExclusive == null ? "null" : p.toKeyExclusive, "UTF-8"));

				if (zeroElement != null) {
					sb.append("&zeroElement=").append(URLEncoder.encode(zeroElement, "UTF-8"));
				}

				String url = sb.toString();
//				System.out.println("operation url sent to flame worker: " + url);
				final int j = i;
				threads[i] = new Thread() {
					public void run() {
						try {
							Response response = HTTP.doRequest("POST", url, lambda);
							responses[j] = response;
							resultsStatusCodes[j] = response.statusCode();
							results[j] = new String(response.body());
//							results[j] = new String(HTTP.doRequest("POST", url, lambda).body());
//							System.out.println("invokeOperation results[" + j + "]: " + results[j]);
//							System.out
//									.println("invokeOperation resultsStatusCodes[" + j + "]: " + resultsStatusCodes[j]);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				threads[i].start();
			}

			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();

				} catch (InterruptedException ie) {
				}
			}

			int accumulator = 0;
			for (int i = 0; i < resultsStatusCodes.length; i++) {
				if (resultsStatusCodes[i] != 200) {
					// something wrong happened to a flame worker when handling operation
					System.out.println("ERR: thread[" + i
							+ "] failure. This means that something wrong happened to a flame worker when handling an operation.");
					return new FlameRDDImpl();
				}
				
				if (operationName.equals("/rdd/fold")) {
//					System.out.println("response body: " + new String(responses[i].body()));
					accumulator += Integer.parseInt(new String(responses[i].body()));
				}
			}

			if (operationName.equals("/rdd/flatMap") 
					|| operationName.equals("/rdd/flatMap")
					|| operationName.equals("/rdd/fromTable")
					|| operationName.equals("/pairRdd/flatMap") 
					|| operationName.equals("/rdd/distinct")
					|| operationName.equals("/rdd/filter")
					|| operationName.equals("/rdd/mappartitions")) {
				FlameRDDImpl flameRDDImpl = new FlameRDDImpl(this.kvsClient, this);
				flameRDDImpl.setInputTable(outputTableName);
				return flameRDDImpl;
			} else if (operationName.equals("/rdd/mapToPair") 
					|| operationName.equals("/pairRdd/foldByKey")
					|| operationName.equals("/rdd/groupBy")
					|| operationName.equals("/rdd/flatMapToPair") 
					|| operationName.equals("/pairRdd/flatMapToPair")
					|| operationName.equals("/pairRdd/join")
					|| operationName.equals("/pairRdd/cogroup")) {
				FlamePairRDDImpl flamePairRDDImpl = new FlamePairRDDImpl(this.kvsClient, this);
				flamePairRDDImpl.setInputTable(outputTableName);
				return flamePairRDDImpl;
			} else if (operationName.equals("/rdd/fold")) {
				return accumulator + "";
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new FlameRDDImpl();
	}

	public FlameRDD invokeIntersection(FlameRDDImpl rdd1, FlameRDDImpl rdd2) {
		String table1 = rdd1.getInputTable();
		String table2 = rdd2.getInputTable();
//		System.out.println("table1: " + table1);
//		System.out.println("table2: " + table2);
		List<String> list1 = new ArrayList<String>();
		List<String> list2 = new ArrayList<String>();

		try {
			Iterator<Row> rows1 = kvsClient.scan(table1, null, null);
			Iterator<Row> rows2 = kvsClient.scan(table2, null, null);
			while (rows1.hasNext()) {
				Row row = rows1.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
				for (String c : columns) {
					byte[] value = this.kvsClient.get(table1, rowKey, c);
					list1.add(new String(value));
				}
			}
			while (rows2.hasNext()) {
				Row row = rows2.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
				for (String c : columns) {
					byte[] value = this.kvsClient.get(table2, rowKey, c);
					list2.add(new String(value));
				}
			}
			String outputTableName = UUID.randomUUID().toString();
//			System.out.println("invokeIntersection() list1: " + list1);
//			System.out.println("invokeIntersection() list2: " + list2);
			Set<String> set = list1.stream().distinct().filter(list2::contains).collect(Collectors.toSet());
//			System.out.println("intersection set: " + set);
			for (String s : set) {
				String rowKey = UUID.randomUUID().toString();
				kvsClient.put(outputTableName, rowKey, "value", s.getBytes());
			}
			FlameRDDImpl flameRDDImpl = new FlameRDDImpl(this.kvsClient, this);
			flameRDDImpl.setInputTable(outputTableName);
			return flameRDDImpl;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new FlameRDDImpl();
	}

	public FlameRDD invokeSampling(String inputTableName, Boolean withReplacement, double probability) {
		try {
			Iterator<Row> rows = kvsClient.scan(inputTableName, null, null);
			int count = kvsClient.count(inputTableName);
			int sampleSize = (int) Math.ceil(count * probability);
			List<String> list = new ArrayList<String>();
			while (rows.hasNext()) {
				Row row = rows.next();
				String rowKey = row.key();
				Set<String> columns = row.columns();
				for (String c : columns) {
					byte[] value = this.kvsClient.get(inputTableName, rowKey, c);
					list.add(new String(value));
				}
			}
//			System.out.println("invokeSampling list: " + list);
			// withReplacment=true means sampling has duplication

			Random rd = new Random();
			List<String> res = new ArrayList<String>();

			if (withReplacement) {
				int n = list.size();

				for (int i = 0; i < sampleSize; i++) {
					int randomIndex = rd.nextInt(n);
					String randomElement = list.get(randomIndex);
					res.add(randomElement);
				}

			} else {
				List<String> listCopy = new ArrayList<String>();
				for (String s : list) {
					listCopy.add(s);
				}
				for (int i = 0; i < sampleSize; i++) {
					int n = listCopy.size();
					int randomIndex = rd.nextInt(n);
					String randomElement = listCopy.get(randomIndex);
					listCopy.remove(randomIndex);
					res.add(randomElement);
				}
			}
//			System.out.println("withReplacement=" + withReplacement + " result list: " + res);

			String outputTableName = UUID.randomUUID().toString();

			for (String s : res) {
				String rowKey = UUID.randomUUID().toString();
				kvsClient.put(outputTableName, rowKey, "value", s.getBytes());
			}
			FlameRDDImpl flameRDDImpl = new FlameRDDImpl(this.kvsClient, this);
			flameRDDImpl.setInputTable(outputTableName);
			return flameRDDImpl;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new FlameRDDImpl();
	}

	public String getOutput() {
		String res = this.sb.toString();
		if (res.length() == 0) {
			return "No Output.";
		}
		return res;
	}

}
