package cis5550.crawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLUtil {

	public static String normalizeSeed(String seed) {
		// normalize a seed url. assume that this is given by absolute path, but could be missing a protocol, port, and an ending slash.
		StringBuilder sb = new StringBuilder();
		String lower = seed.toLowerCase();

		if (!lower.startsWith("http")) {
			sb.append("http://");
		}
		
		sb.append(lower);
		
		if (!lower.endsWith("/")) {
			sb.append("/");
		}
		
		String normalizedSeed = normalizeWithPort(sb.toString());
		
		return normalizedSeed;
	}
	
	public static String normalizeWithPort(String absolutePath) {
		// parent path does NOT end with '/'
		String base = getHost(absolutePath);
		String rest = absolutePath.substring(base.length() + 1);
//		System.out.println("base: " + base + " rest: " + rest);
		StringBuilder basePlusPort = new StringBuilder(base);
		
		Pattern pattern = Pattern.compile(".*:[0-9]+", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(base);
		
		if (!matcher.find()) {
			// url is missing a port
//			System.out.println("base url is missing a port: " + base);
			String defaultPort = base.startsWith("https")? "443": "80";
			basePlusPort.append(":").append(defaultPort);
		}
		String res = basePlusPort.toString() + "/" + rest;
		return res;
	}
	
	public static String getHost(String s) {
		// no trailing /
		StringBuilder base = new StringBuilder();
		int index = s.indexOf("//");
		if (index != -1) {
			base.append(s.substring(0, index+2));
			s = s.substring(index + 2);

//			System.out.println("remove https://: " + s);
			
			int index2 = s.indexOf("/");
			if (index2 != -1) {
				base.append(s.substring(0, index2));
				s = s.substring(index2+1);
			}
		}
		String res = base.toString();
		return res;
	}
	
	public static String normalizeUrl(String base, String url) {
//		System.out.println("normalizing url: " + url);
		int poundIndex = url.indexOf("#");
		String s1 = url;
		if (poundIndex != -1) {
			s1 = url.substring(0, poundIndex);
			if (s1.equals("")) {
//				System.out.println("======");
				return base;
			}
		}
		String baseTrimmed = base.substring(0, base.lastIndexOf("/") + 1);
//		System.out.println("hostTrimmed: " + baseTrimmed);
		
		String host = URLUtil.getHost(base);
		
//		String s2 = baseTrimmed + s1;
//		System.out.println("s2: " + s2);
		String absolutePath="";
		if (!s1.startsWith("/") && !s1.startsWith("http")) {
			// relative url
//			System.out.println("is relative");
			String s2 = baseTrimmed + s1;
//			System.out.println("s2: " + s2);
			absolutePath = removeDots(s2);
		} else if (s1.startsWith("/")) {
			absolutePath = host + s1;
		} else {
			absolutePath = url;
		}
		
		String res = URLUtil.normalizeWithPort(absolutePath);
//		System.out.println("normalized url: " + res);
//		System.out.println("======");
		return res;
	}
	
	private static String removeDots(String s) {
		// remove base url
		// remove trailing "https://..."

		// parent path does NOT end with '/'
		String base = getHost(s);
		String rest = s.substring(base.length()+1);
		if (rest.length() > 0 && rest.charAt(0) == '/') {
			rest = rest.substring(1);
		}
//		System.out.println("base: " + base.toString());
//		System.out.println("rest: " + rest.toString());
		
		Stack<String> stack = new Stack<String>();
		String[] split = rest.split("/");

		for (String sp: split) {
			if (sp.equals("..")) {
				stack.pop();
			} else {
				stack.push(sp);
			}
		}

		List<String> list  = new ArrayList<String>(stack);

//		System.out.println("stack: " + stack);
//		System.out.println("list: " + list);
		
		StringBuilder res = new StringBuilder();
		res.append(base);
		
		for (String e: list) {
			res.append("/").append(e);
		}
//		System.out.println("res: " + res.toString());
		return res.toString();
	}
	
	public static String getHrefLink(String anchor) {
		if (!anchor.contains("href")) {
			return null;
		}
		int index = anchor.indexOf("href");
		String s = anchor.substring(index);
		StringBuilder sb = new StringBuilder();
		char[] arr = s.toCharArray();
		int n = arr.length;
		int i = 0;
		while (i < n && arr[i] != '\"') {
			i++;
		}
		i++;
		while (i < n && arr[i] != '\"') {
			sb.append(arr[i]);
			i++;
		}
		return sb.toString();
				
	}
	

	
	public static void main(String[] args) {
//		String seed = "advanced.crawltest.cis5550.net";
//		String normalizedSeed = normalizeSeed(seed);
//		System.out.println("normalizedSeed: " + normalizedSeed);
		
		List<String> list = new ArrayList<String>();
		list.add("#abc");
		list.add("blah.html#test");
		list.add("../blubb/123.html");
		list.add("/one/two.html");
		list.add("http://elsewhere.com/some.html");
		String base = "https://foo.com:8000/bar/xyz.html";
		for (String s: list) {
			String res = normalizeUrl(base, s);
			System.out.println("original: " + s);
			System.out.println("after: " + res);
		}
		
		list = new ArrayList<String>();
		list.add("a/b.html");
		list.add("/123.html");
		base = "https://foo.com:443/bar/xyz.html";
		for (String s: list) {
			String res = normalizeUrl(base, s);
			System.out.println("original: " + s);
			System.out.println("after: " + res);
		}
	}

}
