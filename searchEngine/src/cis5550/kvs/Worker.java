package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import cis5550.webserver.Request;
import cis5550.webserver.Response;

public class Worker extends cis5550.generic.Worker {
	private static Map<String, Map<String, Row>> workerTables;
	public static Map<String, BufferedOutputStream> workerTablesLog;
	public static Map<Request, Long> requestRecord;
	private int workerPort;
	private String directory;

	public Worker(int workerPort, String directory) {
		Worker.workerTables = new HashMap<String, Map<String, Row>>();
		Worker.workerTablesLog = new HashMap<String, BufferedOutputStream>();
		Worker.requestRecord = new HashMap<Request, Long>();
		this.workerPort = workerPort;
		this.directory = directory;
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.print("incorect command");
			return;
		}

		int workerPort = Integer.valueOf(args[0]);
		String dir = args[1];
		String masterIpPort = args[2];
		String workerId = getWorkerId(dir);

		port(workerPort);
		if (!workerId.isEmpty()) {
			Worker newWorker = new Worker(workerPort, dir);
			newWorker.readWorkerTablesLog();
			newWorker.registerGetColData();
			newWorker.registerGetRow();
			newWorker.registerGetStreamWthStartEnd();
			newWorker.registerGetStreamWoutStartEnd();
			newWorker.registerGetRoot();
			newWorker.registerPut();
			newWorker.registerPutToTable();
			newWorker.registerPutRename();
			newWorker.registerGetRowCount();
			newWorker.registerViewAll();
			startPingThread(masterIpPort, workerId, workerPort);
			startFlushThread(workerTablesLog);
			//EC1
			
			//EC1
//			System.out.println("worker: " + workerId + " working...");
		}

	}
	
	private void registerGetRowCount() {
		get("/count/:tableName", (req,res) -> {
//			System.out.println("register /count/:tableName");
			requestRecord.put(req, System.currentTimeMillis());
			String tableName = req.params("tableName");
			if (workerTables.containsKey(tableName)) {
				String count = String.valueOf(workerTables.get(tableName).size());
//				res.write(count.getBytes());
//				System.out.println("/count/:tablename returning count: " + count);
				return count;
			}else {
				res.status(404, "file not found");
				return 404;
			}
//		return null;
		});	
	}

	private void registerPutRename() {
		put("/rename/:tableName", (req,res) -> {
			requestRecord.put(req, System.currentTimeMillis());
			String tableName = req.params("tableName");
			String newTableName = req.body();
			if (! workerTables.containsKey(tableName)) {
				res.status(404, tableName + " not found");
			}else {
				if (workerTables.containsKey(newTableName)) {
					res.status(409, "new table name already exists");
					return 409;
				}else {
//					rename table, update workerTables
//					move files
//					update workerTablesLog
					Map<String, Row> value = workerTables.get(tableName);
					BufferedOutputStream out = workerTablesLog.get(tableName);
					Path oldPath = Paths.get(tableName + ".table");
					Path newPath = Paths.get(newTableName + ".table");
					
					workerTables.put(newTableName, value);
					workerTables.remove(tableName);
					
					Files.move(oldPath, newPath, StandardCopyOption.REPLACE_EXISTING);
					workerTablesLog.put(newTableName, out);
					workerTablesLog.remove(tableName);
				}
			}	
		return "OK";
		});	
	}

	private void registerPutToTable() {
		put("/data/:tableName", (req,res) -> {
//			System.out.println("put /data/:tableName register");
			requestRecord.put(req, System.currentTimeMillis());
			String tableName = req.params("tableName");
			String dir = this.directory;

			BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(req.bodyAsBytes()));
			Row newRow;
			
			while ((newRow = Row.readFrom(bis)) != null) {
				String rowKey = newRow.key();
				
				if (!workerTables.containsKey(tableName)) {
					Map<String, Row> newEntry = new HashMap<String, Row>();
					newEntry.put(rowKey, newRow);
					workerTables.put(tableName, newEntry);
					updateLogFile(dir, tableName, newRow);
				}else {
					workerTables.get(tableName).put(rowKey, newRow);
					updateLogFile(dir, tableName, newRow);
				}	
			}	
		return "OK";
		});
	}

	private void readWorkerTablesLog() {
		String dir = this.directory;
		File file = new File(dir);
		File[] files = file.listFiles();
		List<File> tableFiles = new ArrayList<File>();
		for (File f : files) {
			if (f.getPath().endsWith(".table")) {
				tableFiles.add(f);
			}
		}
		for (File f : tableFiles) {
			String[] pathArr = f.getAbsolutePath().split("/");
			String fileName = null;
			for (int i = 0; i < pathArr.length; i++) {
				if (pathArr[i].endsWith(".table")) {
					fileName = pathArr[i];
				}
			}
			int ind = fileName.lastIndexOf(".");
			String tableName = fileName.substring(0, ind);
			workerTables.put(tableName, new HashMap<String, Row>());

			try {
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f.getAbsolutePath()));
				Row newRow;
				while ((newRow = Row.readFrom(bis)) != null) {
					String newRowKey = newRow.key();
					workerTables.get(tableName).put(newRowKey, newRow);
				}

			} catch (Exception e1) {
				System.out.println("error read new row from " + fileName);
				e1.printStackTrace();
			}
		}
	}
	
	private synchronized void createTableIfNotExist(String tableName) throws FileNotFoundException {
		if (!workerTables.containsKey(tableName)) {
			workerTables.put(tableName, new HashMap<String, Row>());
			File newLog = new File(tableName + ".table");
			String logPath = this.directory + "/" + tableName + ".table";
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(logPath, true));
			workerTablesLog.put(tableName, out);
			
		}
	}

	private void registerGetStreamWthStartEnd() {
		get("/data/:tableName/:startRow/:endRowExclusive", (req, res) -> {
			requestRecord.put(req, System.currentTimeMillis());
			String tableName = req.params("tableName");
			String startRow = req.params("startRow");
			String endRowExclusive = req.params("endRowExclusive");
			createTableIfNotExist(tableName);
			for (String rowKey : workerTables.get(tableName).keySet()) {
				if (rowKey.compareTo(startRow) >= 0 && rowKey.compareTo(endRowExclusive) < 0) {
					getStreamFromTo(res, tableName, startRow, endRowExclusive);
				}
			}
			return null;
		});

	}

	private void registerGetStreamWoutStartEnd() {
		get("/data/:tableName", (req, res) -> {
//			System.out.println("/data/:tableName register route.");
			requestRecord.put(req, System.currentTimeMillis());
			String tableName = req.params("tableName");
			String startRow = req.queryParams("startRow");
			String endRowExclusive = req.queryParams("endRowExclusive");
//			System.out.println("/data/:tableName register tableName: " + tableName + " startRow: " + startRow + " endRowExclusive: " + endRowExclusive);
//			createTableIfNotExist(tableName);
//			String endRowExclusive = String.valueOf(workerTables.get(tableName).size());
//			String startRow = String.valueOf(0);
//			getStreamFromTo(res, tableName, startRow, endRowExclusive);
			Map<String, Row> map = workerTables.get(tableName);
			if (map == null) {
				res.status(404, "Not Found");
				return "Table " + tableName + " Not Found";
			}
			StringBuilder sb = new StringBuilder();
			for (String rowKey: map.keySet()) {
				if ((startRow == null || startRow.compareTo(rowKey) <= 0) && (endRowExclusive == null || endRowExclusive.compareTo(rowKey) > 0)) {
					Row row = map.get(rowKey);
					res.write(row.toByteArray());
					res.write("\n".getBytes());
				}
			}
			res.write("\n".getBytes());
			return null;
		});
	}

	private void getStreamFromTo(Response res, String tableName, String startRow, String endRowExclusive) {
		if (workerTables.containsKey(tableName)) {
			ArrayList<String> keySet = new ArrayList<String>();

			if (Integer.valueOf(startRow) == 0
					&& Integer.valueOf(endRowExclusive) == workerTables.get(tableName).size()) {
				keySet.addAll(workerTables.get(tableName).keySet());
			} else {
				for (String rowKey : workerTables.get(tableName).keySet()) {
					if (rowKey.compareTo(startRow) >= 0 && rowKey.compareTo(endRowExclusive) < 0) {
						keySet.add(rowKey);
					}
				}
			}

			try {
				for (String rowKey : keySet) {
					byte[] bodyRowAsBytes = workerTables.get(tableName).get(rowKey).toByteArray();
//					System.out.println("writing...." + workerTables.get(tableName).get(rowKey).toString());
					res.write(bodyRowAsBytes);
					res.write("\n".getBytes());
				}
				res.write("\n".getBytes());
				res.status(200, "OK");
			} catch (Exception e) {
				System.out.println("error write " + tableName + "to stream");
				e.printStackTrace();
			}
		} else {
			res.status(404, "file not found");
		}
	}

	private void registerGetRow() {
		get("/data/:tableName/:row", (req, res) -> {
			requestRecord.put(req, System.currentTimeMillis());
			
			String tableName = req.params("tableName");
			String rowKey = req.params("row");
			if (workerTables.containsKey(tableName) && workerTables.get(tableName).containsKey(rowKey)) {
				byte[] body = workerTables.get(tableName).get(rowKey).toByteArray();
				res.bodyAsBytes(body);
				res.status(200, "OK");
			} else {
				res.status(404, "File not found");
				System.out.println("requested file not found");
			}
			return null;
		});
	}
	
	private void registerViewAll() {
		get("/view/:tableName", (req, res) -> {
			requestRecord.put(req, System.currentTimeMillis());
			String host = req.headers("host");
			String tableName = req.params("tableName");
			Map<String, Row> viewingTable = workerTables.get(tableName);
			Map<String, Row> viewingTableSorted = new TreeMap<>(viewingTable);
//			System.out.println("table size: " + viewingTableSorted.size());
//			for (String k: viewingTableSorted.keySet()) {
//				Row v = viewingTableSorted.get(k);
//				System.out.println("rowkey: " + v.key);
//				for (String c: v.columns()) {
//					if (!c.equals("page")) {
//						System.out.println("column: " + c + " content: " + new String(v.get(c)));
//					}
//				}
//				System.out.println("======");
//			}
			String startRow = req.queryParams("startRow");
			startRow = startRow == null || Integer.valueOf(startRow) > viewingTableSorted.size()? String.valueOf(0) : startRow;

			int[] size= {viewingTableSorted.size(), 10};
			int pageSize = size[0] < size[1]? size[0] : size[1];
			int start = Integer.valueOf(startRow);
			boolean hasNextPage = viewingTableSorted.size() - Integer.valueOf(startRow) > pageSize? true: false;
			
			String[] pageOne = viewingTableSorted.keySet().toArray(new String[viewingTableSorted.size()]);
			ArrayList<String> pageOneRKeys = new ArrayList<String>();
			
			int i;
			for (i = start; i < pageSize+start && i < viewingTableSorted.size() ; i++) {
				pageOneRKeys.add(pageOne[i]);
			}
			
			int newPageStartRow = i;
			
			String HTML = generateHTML(host, tableName, pageOneRKeys, hasNextPage, newPageStartRow);
			res.header("Content-Type", "text/html");
			res.body(HTML);
			res.status(200, "OK");
			return null;
		});
	}

	private String generateHTML(String host, String tableName, ArrayList<String> currentPageRKeys, boolean hasNextPage, int newPageStartRow) {
		StringBuilder sb = new StringBuilder();
		sb.append("<!doctype html>\n");
		sb.append("<html>\n");
		sb.append("<head>\n");
		sb.append("<title>");
		sb.append(tableName);
		sb.append("</title>\n");
		sb.append("<style>\ntable, th, td {\nborder: 0.5px solid black;\n}\n");
		sb.append("table.center {\nmargin-left: auto;\nmargin-right: auto;\n}\n</style>\n");
		sb.append("</head>\n");
		sb.append("<body>\n");
		sb.append("<h2>");
		sb.append("viewing ").append(tableName).append(" content");
		sb.append("</h2>\n");
		sb.append("<table class=\"center\" style=\"width:50%\">\n");
		
		if (currentPageRKeys.size() > 0) {
			sb.append("<table>\n");
			sb.append("<tr>\n");
			String rowKey = currentPageRKeys.get(0);
			sb.append("<th>").append(rowKey).append("</th>\n");
			Row currentRow = workerTables.get(tableName).get(rowKey);
			Set<String> colKeySet = currentRow.values.keySet();
			List<String> colKeySetSorted = new ArrayList<>(colKeySet);
			Collections.sort(colKeySetSorted);
			for (String cKey: colKeySetSorted) {
				sb.append("<th>").append(cKey).append("</th>\n");
			}
			sb.append("</tr>\n");
		}
		
//		sb.append("<table>\n");
//		sb.append("<tr>\n");
//		sb.append("<th>id</th>\n");
//		sb.append("<th>ip</th>\n");
//		sb.append("<th>port</th>\n");
//		sb.append("<th>link</th>\n");
//		sb.append("</tr>\n");

		for (int i = 0; i < currentPageRKeys.size(); i++) {
			String rowKey = currentPageRKeys.get(i);
			Row currentRow = workerTables.get(tableName).get(rowKey);
			String rKey = currentRow.key;
			Set<String> colKeySet = currentRow.values.keySet();
			List<String> colKeySetSorted = new ArrayList<>(colKeySet);
			Collections.sort(colKeySetSorted);
			sb.append("<tr style=\\\"background-color:#FAF0F0\\\">");
			sb.append("<td>").append(rKey).append("</td>\n");
			for (String cKey : colKeySetSorted) {
				
				byte[] cell = currentRow.values.get(cKey);
				String cellContent = new String(cell);
//				HTML.append("<tr style=\"background-color:#FAF0F0\">");
//				HTML.append("<th>").append("Row Key ").append(":").append("</th>");
//				HTML.append("<th>").append("Column Key:  ").append(cKey).append("</th>");
//				HTML.append("</tr>\n");

//				HTML.append("<tr>");
//				HTML.append("<th>").append(rKey).append("</th>");
//				HTML.append("<th>").append(cellContent).append("</th>");
				sb.append("<td>").append(cellContent).append("</td>\n");
//				HTML.append("</tr>\n");
//				HTML.append("<tr></tr>");

			}
			sb.append("</tr>\n");
		}
		sb.append("</table>");
		
		String nextPageUrl;
		if (!hasNextPage) {
			sb.append("<p>End.</p>");	
		}else {
			if (!host.matches("(.*):(.*)")) {
				nextPageUrl = "/view/" + tableName + "?startRow=" + newPageStartRow;
			}else {
				nextPageUrl = "/view/" + tableName + "?startRow=" + newPageStartRow;
			}
			sb.append("<a href=\"" + nextPageUrl + "\">Next Page</a>\n");	
		}
		sb.append("</body>\n");
		sb.append("</html>\n");
		
		return sb.toString();
	}
	
	private void registerGetRoot() {
		get("/", (req, res) -> {
			requestRecord.put(req, System.currentTimeMillis());
//			generate HTML tags
			StringBuilder sb = new StringBuilder();
			sb.append("<!doctype html>\n");
			sb.append("<html>\n");
			sb.append("<head>\n");
			sb.append("<title>");
			sb.append("Hello Workers!");
			sb.append("</title>\n");
			sb.append("</head>\n");
			sb.append("<style>\n");
			sb.append("table {border-collapse: collapse; width: 100%;}\n");
			sb.append("td, th {border: solid; text-align: left; padding: 8px;}\n");
			sb.append("tr:nth-child(even) {background-color: #dddddd;}\n");
			sb.append("</style>\n");
			sb.append("<body>\n");
			sb.append("<h2>");
			sb.append("Worker Table List");
			sb.append("</h2>\n");
			sb.append("<table style=\"width:100%\">\n");
			sb.append("<tr>\n");
			sb.append("<th>");
			sb.append("Table Name");
			sb.append("</th>\n");
			sb.append("<th>");
			sb.append("View Table Content");
			sb.append("</th>\n");
			sb.append("<th>");
			sb.append("Number of Keys");
			sb.append("</th>\n");
			sb.append("</tr>\n");
			for (String tableName : workerTables.keySet()) {
				sb.append("<tr>");
				sb.append("<th>");
				sb.append(tableName);
				sb.append("</th>");
				sb.append("<th>");
				sb.append("<a href=\"/view/");
				sb.append(tableName);
				sb.append("\">");
				sb.append("link");
				sb.append("</a>");
				sb.append("</th>");
				sb.append("<th>");
				sb.append(workerTables.get(tableName).size());
				sb.append("</th>");
				sb.append("</tr>");
			}
			sb.append("</table>");
			sb.append("<p>End of Worker Table</p>");
			sb.append("</body>");
			sb.append("</html>");
			res.body(sb.toString());
			res.status(200, "OK");
			res.header("Content-Type", "text/html");
			return null;
		});
	}

	private void updateLogFile(String dir, String tableName, Row newRow) {
		String logPath = dir + "/" + tableName + ".table";

		if (!workerTablesLog.containsKey(tableName)) {
//			create a new workerTablesLog map entry

			File newLog = new File(tableName + ".table");
			if (!newLog.exists()) {
				newLog.mkdir();
			}
			try {
				BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(logPath, true));
				workerTablesLog.put(tableName, out);
				out.write(newRow.toByteArray());
				out.write("\n".getBytes());
//				System.out.print("write success\n");
			} catch (IOException e) {
				System.out.println("error writing to " + tableName + ".table");
			}
		} else {
			BufferedOutputStream out = workerTablesLog.get(tableName);
			try {
				out.write(newRow.toByteArray());
				out.write("\n".getBytes());
			} catch (IOException e) {
				System.out.println("error writing to existing " + tableName + ".table");
			}

		}

	}

	private void registerPut() {
		put("/data/:tableName/:row/:col", (req, res) -> {
//			System.out.println("put /data/:tableName/:row/:col register");
			requestRecord.put(req, System.currentTimeMillis());
//			String tableName = req.params("tableName");
//			URLDecoder.decode(request.queryParams("inputTableName1"), "UTF-8")
			String tableName = URLDecoder.decode(req.params("tableName"), "UTF-8");
//			String rowKey = req.params("row");
			String rowKey = URLDecoder.decode(req.params("row"), "UTF-8");
//			String colKey = req.params("col");
			String colKey = URLDecoder.decode(req.params("col"), "UTF-8");
			String dir = this.directory;
			byte[] body = req.bodyAsBytes();
			Row newRow = null;
//			System.out.println("trying to put tableName: " + tableName + " rowKey: " + rowKey + " colKey: " + colKey);
			if (!workerTables.containsKey(tableName)) {
				workerTables.put(tableName, new HashMap<String, Row>());

			}
//			System.out.println("workerTables: " + workerTables);
			if (!workerTables.get(tableName).containsKey(rowKey)) {
				newRow = new Row(rowKey);
				workerTables.get(tableName).put(rowKey, newRow);
			}
			Row row = workerTables.get(tableName).get(rowKey);
			synchronized (row) {
				workerTables.get(tableName).get(rowKey).put(colKey, body);
			}
			updateLogFile(dir, tableName, workerTables.get(tableName).get(rowKey));
			
//			System.out.println("put success.");
			return "OK";
		});
	}

	private void registerGetColData() {
		get("/data/:tableName/:row/:col", (req, res) -> {
//			System.out.println("/data/:tablename/:row/:col get row register");
			requestRecord.put(req, System.currentTimeMillis());
//			String tableName = req.params("tableName");
			String tableName = URLDecoder.decode(req.params("tableName"), "UTF-8");
//			String row = req.params("row");
			String row = URLDecoder.decode(req.params("row"), "UTF-8");
//			String col = req.params("col");
			String col = URLDecoder.decode(req.params("col"), "UTF-8");
			if (!workerTables.containsKey(tableName) || !workerTables.get(tableName).containsKey(row)) {
				String reasonPhrase = "entry=" + tableName + " not found.";
				res.status(404, reasonPhrase);
				return reasonPhrase;
			} else {
				Row currentRow = workerTables.get(tableName).get(row);
//				System.out.println("/data/:tablename/:row/:col get row: " + currentRow);
				byte[] body = currentRow.getBytes(col);
				
				res.bodyAsBytes(body);
			}
			return null;
		});
	}

	private static String getWorkerId(String dir) {
		String path = dir + "/id";
		String workerId = "";

		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			try {
				workerId = br.readLine();
				br.close();
			} catch (IOException e) {
				System.out.println("error reading file");
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			System.out.println("no file found. creating new worker id file...");
			File workerIdFile = new File(dir);
			if (!workerIdFile.exists()) {
				workerIdFile.mkdir();
			}
			workerId = generateRandomString(5);
			try {
				PrintWriter pw = new PrintWriter(path);
				pw.println(workerId);
				pw.close();
			} catch (FileNotFoundException e1) {
				System.out.println("cannot write to path" + path);
				e1.printStackTrace();
			}
		}
		return workerId;
	}

	private static String generateRandomString(int len) {
		String lettersPool = "abcdefghijklmnopqrstuvwxyz";
		int poolLen = lettersPool.length();
		Random rd = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			int index = rd.nextInt(poolLen);
			sb.append(lettersPool.charAt(index));
		}
		return sb.toString();
	}
}
