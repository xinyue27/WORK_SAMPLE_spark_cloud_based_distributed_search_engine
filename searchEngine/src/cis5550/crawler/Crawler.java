package cis5550.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Crawler {
	static Map<String, String> robots = new HashMap<String, String>(); // key: host, value: raw robots.txt string

	public static void run(FlameContext ctx, String args[]) {
		System.out.println("Cralwer.run()");
		if (args.length != 1) {
			ctx.output("ERR: args does not contain a single element of seed URL.");
			return;
		}
		List<String> list = new ArrayList<String>();
		
		String seed = args[0];
		String normalizedSeed = URLUtil.normalizeSeed(seed);
		list.add(normalizedSeed);
		System.out.println("list: " + list);
		
		
		
		String userAgent = "cis5550-crawler";
		
		try {
			FlameRDD urlQueue = ctx.parallelize(list);
			System.out.println("urlQueue: " + urlQueue);
			while (urlQueue.count() != 0) {
				urlQueue = urlQueue.flatMap(url -> {
					try {
//					List<String> res = new ArrayList<String>();
					System.out.println("handling url: " + url);
//					String hashedUrl = Hasher.hash(url);
					String rowKey = Hasher.hash(url);
//					System.out.println("rowKey: " + rowKey);
//					KVSClient kvsClient = FlameContext.getKVS();
					KVSClient kvsClient = new KVSClient("localhost:8000");
//					System.out.println("FlameContext: " + FlameContext);
//					System.out.println("FlameContext.getKvs(): " + FlameContext.getKVS());
//					System.out.println("crawler get kvsClient: " + kvsClient);
					
					// remove contentsSeen
//					if (kvsClient.existsRow("crawl", rowKey)) {
//						System.out.println("already crawled url: " + url);
//						return new ArrayList<String>();
//					}
					
					String host = URLUtil.getHost(url);
//					System.out.println("crawler host: " + host);
					
					boolean isOkToCrawl = isOkToCrawl(userAgent, host, url);
					System.out.println("isOkToCrawl: " + isOkToCrawl);
					
					if (!isOkToCrawl) {
//						System.out.println("this url is not ok to crawl: " + url);
						Row row = new Row(rowKey);
						row.put("url", url);
						row.put("Banned?", "True");
						kvsClient.putRow("banned", row);
						return new ArrayList<String>();
					}
					
					boolean shouldThrottle = shouldThrottleCrawl(kvsClient, host);
					System.out.println("shouldThrottle: " + shouldThrottle);
					if (shouldThrottle) {
//						System.out.println("throttling crawling url: " + url);
						List<String> res = new ArrayList<String>();
						res.add(url);
						return res;
					}
					
//					System.out.println("not throttling");
					
					Long currTimeStamp = System.currentTimeMillis();
					kvsClient.put("hosts", host, "lastAccessed", currTimeStamp + "");
					
//					System.out.println("making head request");
					HttpURLConnection headConn = (HttpURLConnection) new URL(url).openConnection();
					headConn.setRequestMethod("HEAD");
					headConn.setRequestProperty("User-Agent", userAgent);
					try {
						headConn.connect();
					} catch (Exception e) {
						return new ArrayList<String>();
					}
					int headResponseCode = headConn.getResponseCode();
					String contentType = headConn.getContentType();
					int contentLength = headConn.getContentLength();
					
					String tableName = "crawl";
//					String rowKey = Hasher.hash(url);
					Row row = new Row(rowKey);
					row.put("url", url);
					row.put("responseCode", headResponseCode+"");
					if (contentType != null) {
						
						row.put("contentType", contentType);
					}
					if (contentLength != -1) {
						row.put("contentLength", contentLength+"");
					}
					
					kvsClient.putRow(tableName, row);
//					System.out.println("putting conn info tableName: " + tableName + " row: " + row);
//					System.out.println("headResponseCode: " + headResponseCode);
					if (headResponseCode == 301 || headResponseCode == 307 || headResponseCode == 308) {
//						System.out.println("redirect");
						String redirectUrl = headConn.getHeaderField("location");
//						System.out.println("redirectUrl: " + redirectUrl);
						String normalizedRedirectUrl = URLUtil.normalizeUrl(url, redirectUrl);
//						System.out.println("normalizedRedirectUrl: " + normalizedRedirectUrl);
						List<String> res = new ArrayList<String>();
						res.add(normalizedRedirectUrl);
						return res;
					}
					
					if (headResponseCode != 200) {
						return new ArrayList<String>();
					}
					
//					System.out.println("making get request");

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
					conn.setRequestMethod("GET");
					conn.setRequestProperty("User-Agent", userAgent);
//					con.setRequestProperty("Content-Type", "application/jar-archive");
					conn.connect();
					

					
					int responseCode = conn.getResponseCode();
					if (responseCode != 200) {
//						System.out.println("ERR: responseCode is not 200 when connecting to url: " + url);
						return new ArrayList<String>();
					}
					// override response code to be head response code
					kvsClient.put(tableName, rowKey, "responseCode", responseCode+"");
					StringBuilder sb = new StringBuilder();
					BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
					String line;
					while ((line = br.readLine()) != null) {
//						System.out.println("reading line: " + line);
						sb.append(line).append("\n");
					}
					String webPageContent = sb.toString();
//					String rowKey = Hasher.hash(url);
//					Row row = new Row(rowKey);
					String columnName = "page";
//					row.put(columnName, webPageContent);
					
					// only put in page column is contentType is text/html
//					System.out.println("contentType: " + contentType);
					if (contentType != null && contentType.startsWith("text/html")) {
//						kvsClient.put(tableName, rowKey, columnName, webPageContent);
//						System.out.println("putting content");
						// also put in crawled content table for EC1
						String hashedContent = Hasher.hash(webPageContent);
						String contentSeenTable = "contentSeen";
						
						if (kvsClient.existsRow(contentSeenTable, hashedContent)) {
//							System.out.println("the same content has been crawled before");
							byte[] canonicalUrl = kvsClient.get(contentSeenTable, hashedContent, "url");
//							String canonicalUrl = new String(canonicalUrlBytes);
							kvsClient.put(tableName, rowKey, "canonicalURL", canonicalUrl);
						} else {
//							System.out.println("this content has not been crawled before");
							kvsClient.put(tableName, rowKey, columnName, webPageContent);
							kvsClient.put(contentSeenTable, hashedContent, "url", url);
						}
					}
//					currTimestamp = System.currentTimeMillis();
//					Timestamp ts
//		            = new Timestamp(System.currentTimeMillis());
//		 
//		        // Passing the long value in the Date class
//		        // constructor
//		        Date date = new Date(ts.getTime());
		        	Date date = new Date((new Timestamp(System.currentTimeMillis())).getTime());
		        	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					String ts = sdf.format(date);
		        	kvsClient.put(tableName, rowKey, "TimeStamp", ts);
					
					
					List<String> children = extractUrls(url, webPageContent);
//					System.out.println("children: " + children);
					return children;
					} catch (Exception e) {
						return new ArrayList<String>();
					}
				});
//				Thread.sleep(1000);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: FlameContext parallelize error: " + e);
			e.printStackTrace();

		}
		
		

		
//		ctx.output("OK. seed url: " + args[0]);

		
	}
	
	private static boolean isOkToCrawl(String userAgent, String host, String url) {
		try {
		String rules = "";
		if (!robots.containsKey(host) ) {
//			System.out.println("make get request to get robots.txt for host: " + host);
			rules = getRobotRules(host);
//			String rules = "User-agent: cis5550-crawler\nAllow: /abc\nDisallow: /a\n".toLowerCase();
//			String rules = "User-agent: cis5550-crawler\nDisallow: /a\nallow: /abc\n".toLowerCase();
			robots.put(host, rules);
		} else {
//			System.out.println("robot rules are already cached for host: " + host);
			rules = robots.get(host);
		}
		if (rules.trim().equals("")) {
				// no rules
				return true;
			}
			Map<String, List<String>> rulesMap = parseRobotRules(host, rules);
			
//			Map<String, Set<String>> allows = this.allowRules.get(host);
//			Map<String, Set<String>> disallows = this.allowRules.get(host);
			
			if (rulesMap.containsKey(userAgent)) {
//				System.out.println("contains rule for user agent: " + userAgent);
				List<String> userAgentRules = rulesMap.get(userAgent);
				for (String s: userAgentRules) {
					String[] split = s.split(":");
					String flag = split[0].trim();
					String value = split[1].trim();
					
					if (value.startsWith("/")) {
						String urlRelativePath = url.substring(host.length());
//						System.out.println("urlRelativePath: " + urlRelativePath);
						if (flag.equals("allow") && urlRelativePath.startsWith(value)) {
//							System.out.println("allow this userAgent, urlRelativePath: " + urlRelativePath + ", value: " + value);
							return true;
						}
						if (flag.equals("disallow") && urlRelativePath.startsWith(value)) {
//							System.out.println("ban this userAgent, urlRelativePath: " + urlRelativePath + ", value: " + value);
							return false;
						}
						
					}
				}

			}
			
			if (rulesMap.containsKey("*")) {
//				System.out.println("contains rule for all user agent.");
				List<String> userAgentRules = rulesMap.get("*");
				for (String s: userAgentRules) {
					
					String[] split = s.split(":");
					String flag = split[0].trim();
					String value = split[1].trim();
					
					if (value.startsWith("/")) {
						String urlRelativePath = url.substring(host.length());
//						System.out.println("urlRelativePath: " + urlRelativePath);
						if (flag.equals("allow") && urlRelativePath.startsWith(value)) {
//							System.out.println("allow all userAgent, urlRelativePath: " + urlRelativePath + ", value: " + value);
							return true;
						}
						if (flag.equals("disallow") && urlRelativePath.startsWith(value)) {
//							System.out.println("ban all userAgent, urlRelativePath: " + urlRelativePath + ", value: " + value);
							return false;
						}
						
					}
					
				}

			}
		} catch(Exception e) {
			return false;
		}
				
		
		return true;
	}
	
	private static Map<String, List<String>> parseRobotRules(String host, String rules) {
//		Map<String, Set<String>> allows = new HashMap<String, Set<String>>();
//		Map<String, Set<String>> disallows = new HashMap<String, Set<String>>();
		Map<String, List<String>> rulesMap = new HashMap<String, List<String>>();
		String[] lines = rules.split("[\r\n]+");
		String currUserAgent="";
		for (String line: lines) {
//			System.out.println("line: " + line);
			if (line.startsWith("user-agent")) {
				String[] split = line.split(":");
				String userAgent = split[1].trim();
				currUserAgent = userAgent;
				if (!rulesMap.containsKey(currUserAgent)) {
					rulesMap.put(currUserAgent, new ArrayList<String>());
				}
				
//					if (!allows.containsKey(userAgent)) {
//						allows.put(userAgent, new HashSet<String>());
//					}
//					if (!disallows.containsKey(userAgent)) {
//						disallows.put(userAgent, new HashSet<String>());
//					}
				
				
			} else if (line.startsWith("allow")) {
//				String[] split = line.split(":");
//				String allow = split[1].trim();
//				allows.get(currUserAgent).add(allow);
				rulesMap.get(currUserAgent).add(line);
				
			} else if (line.startsWith("disallow")) {
//				String[] split = line.split(":");
//				String disallow = split[1].trim();
//				disallows.get(currUserAgent).add(disallow);
				rulesMap.get(currUserAgent).add(line);
			}
			// ignore the rest of the rules
		}
//		System.out.println("allows: " + allows);
//		System.out.println("disallows: " + disallows);
//		System.out.println("rules map: " + rulesMap);
//		if (!this.allowRules.containsKey(host)) {
//			
//			this.allowRules.put(host, allows);
//		}
//		if (!this.disallowRules.containsKey(host)) {
//			this.disallowRules.put(host, disallows);
//		}
		return rulesMap;
	}
	
	private static String getRobotRules(String host) {
		// this will be in lower case.
		String robotUrl = host + "/robots.txt";
//		System.out.println("getting robot rules at: " + robotUrl);
		HttpURLConnection conn;
		try {
			conn = (HttpURLConnection) new URL(robotUrl).openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("User-Agent", "cis5550-crawler");
			try {
			conn.connect();
			} catch (Exception e) {
				return "";
			}
			
			int responseCode = conn.getResponseCode();
			if (responseCode != 200) {
				System.out.println("ERR: responseCode is not 200 when connecting to robot url: " + robotUrl);
				// treating this as no rules for this host.
				return "";
			}
			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = br.readLine()) != null) {
//				System.out.println("reading line: " + line);
				sb.append(line).append("\n");
			}
			String rules = sb.toString();
//			System.out.println("robot rules: " + rules);
			return rules.toLowerCase();
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: failure connecting to robot url: " + robotUrl + ", e: " + e);
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: failure connecting to robot url: " + robotUrl + ", e: " + e);
			e.printStackTrace();
		}
		return "";

	}
	private static List<String> extractUrls(String base, String webPageContent) {
//		System.out.println("extractUrls");
		List<String> res = new ArrayList<String>();
//		found an href: <a href="/UJ2p.html">hisgravity!"He</a>
//				found an href: <a href="/LE4.html">it</a>
//				found an href: <a href="/ND3.html">secretest</a>
//				found an href: <a href="/">as</a>
//				found an href: <a href="/ItU5tEu.html">difficult,and</a>
//				found an href: <a href="/ItU5tEu.html">Bruenn's</a>
//				found an href: <a href="/SL7sCi.html">shown</a>
//				found an href: <a href="/ItU5tEu.html">kissing</a>
//				found an href: <a href="/SL7sCi.html">the</a>
//				found an href: <a href="/">to</a>
//				found an href: <a href="/xr6yf.html">know</a>
//				found an href: <a href="/UJ2p.html">has</a>
//				found an href: <a href="/ItU5tEu.html">room.</a>
		Pattern pattern = Pattern.compile("(<a[^>]+>.+?<\\/a>)", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(webPageContent);
		while(matcher.find()) {
			String anchor = matcher.group();
//			System.out.println("found an anchor: " + anchor);
			String link = URLUtil.getHrefLink(anchor);
			if (link != null) {
				String normalizedUrl = URLUtil.normalizeUrl(base, link);
				if (!shouldFilter(normalizedUrl)) {
					res.add(normalizedUrl);
				}
			}
		}
		return res;
		
	}
	

	
	private static boolean shouldFilter(String url) {
		if (!url.startsWith("http://") && !url.startsWith("https://")) {
			return true;
		}
		if (url.endsWith(".jpg") || url.endsWith(".jpeg")
				|| url.endsWith(".gif") || url.endsWith(".png") || url.endsWith(".txt")) {
			return true;
		}
		return false;
	}

	
	private static boolean shouldThrottleCrawl(KVSClient kvsClient, String host) {
		long currTimeStamp = System.currentTimeMillis();
		
		byte[] lastAccessed;
		try {
			lastAccessed = kvsClient.get("hosts", host, "lastAccessed");
			System.out.println("lastAccessed: " + lastAccessed);
			
			if (lastAccessed != null) {
				long lastAccessedTimeStamp = Long.parseLong(new String(lastAccessed));
				System.out.println("lastAccessedTimeStamp: " + lastAccessedTimeStamp);
				long diff = currTimeStamp - lastAccessedTimeStamp;
				System.out.println("diff: " + diff);
				if (diff < 1000) {
					// throttling crawling
					return true;
				}
			}
		} catch (IOException e) {
			// just allow it to crawl when this happens.
			System.out.println("ERR: failure in getting lastAccessed in hosts table.");
			return false;
		}

		return false;
	}
	

	
	public static void main(String[] args) {
		//http://simple.crawltest.cis5550.net/
		//http://advanced.crawltest.cis5550.net/
		String response;
		System.out.println("crawling seed: " + args[0]);
		try {
			response = FlameSubmit.submit("localhost:9000", "src/cis5550/crawler/crawler.jar", "cis5550.crawler.Crawler", args);
			System.out.println("response:");
			System.out.println(response);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		List<String> list = new ArrayList<String>();
//		list.add("#abc");
//		list.add("blah.html#test");
//		list.add("../blubb/123.html");
//		list.add("/one/two.html");
//		list.add("http://elsewhere.com/some.html");
//		String base = "https://foo.com:8000/bar/xyz.html";
//		for (String s: list) {
//			String res = normalizeUrl(base, s);
//			System.out.println("original: " + s);
//			System.out.println("after: " + res);
//		}
//		
//		list = new ArrayList<String>();
//		list.add("a/b.html");
//		list.add("/123.html");
//		base = "https://foo.com:443/bar/xyz.html";
//		for (String s: list) {
//			String res = normalizeUrl(base, s);
//			System.out.println("original: " + s);
//			System.out.println("after: " + res);
//		}
		
//		List<String> list = new ArrayList<String>();
//		String anchor = "<a href=\"/UJ2p.html\">hisgravity!\"He</a>";
//		list.add(anchor);
//		anchor = "<a href=\"/LE4.html\">it</a>";
//		list.add(anchor);
//		for (String s: list) {
//			String res = getHrefLink(s);
//			System.out.println("res: " + res);
//		}
		
//		Crawler crawler = new Crawler();
//		boolean flag = crawler.isOkToCrawl("http://simple.crawltest.cis5550.net:80", "http://simple.crawltest.cis5550.net:80/abcdef");
//		System.out.println("flag: " + flag);
//		flag = crawler.isOkToCrawl("http://simple.crawltest.cis5550.net:80", "http://simple.crawltest.cis5550.net:80/xyz");
//		System.out.println("flag: " + flag);
//		flag = crawler.isOkToCrawl("http://simple.crawltest.cis5550.net:80", "http://simple.crawltest.cis5550.net:80/alpha");
//		System.out.println("flag: " + flag);
	}
	
}
