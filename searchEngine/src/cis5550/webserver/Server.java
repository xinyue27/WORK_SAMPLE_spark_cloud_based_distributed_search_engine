package cis5550.webserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Server extends Thread {
	private static Server server = null;
	private static boolean flag = false;
	public static int httpPort = 80;
	public static int httpsPort = -1;
	private String dir = null;

	public static List<RoutingTableEntry> routingTableWithHost;
	public static List<RoutingTableEntry> routingTableWithoutHost;

	public static List<Filter> beforeFilters;
	public static List<Filter> afterFilters;
	
	public static Map<String, SessionImpl> sessionsTable;
	public static String host;
	
	private Server() {
//		System.out.println("Server instantiated, this should only happen once.");

		routingTableWithHost = new ArrayList<RoutingTableEntry>();
		routingTableWithoutHost = new ArrayList<RoutingTableEntry>();

		beforeFilters = new ArrayList<Filter>();
		afterFilters = new ArrayList<Filter>();
		
		sessionsTable = new ConcurrentHashMap<String, SessionImpl>();
	}
	
	private static Server getInstance() {
		if (server == null) {
			server = new Server();
		}
		return server;
	}
	
	public void run() {
		ServerController serverController = new ServerController(httpPort, httpsPort, dir);
		serverController.start();
	}
	
	public static void port(int port) {
		Server.httpPort = port;
//		Server.httpsPort = port;
	}
	
	public static void get(String path, Route route) {
		if (!flag) {
			flag = true;
			getInstance().start();
		}

		if (host == null) {
			routingTableWithoutHost.add(new RoutingTableEntry("GET", path, route, ""));	
		} else {
			routingTableWithHost.add(new RoutingTableEntry("GET", path, route, host));	
		}
		
	}
	public static void put(String path, Route route) {
		if (!flag) {
			flag = true;
			getInstance().start();
		}

		if (host == null) {
			routingTableWithoutHost.add(new RoutingTableEntry("PUT", path, route, ""));	
		} else {
			routingTableWithHost.add(new RoutingTableEntry("PUT", path, route, host));	
		}
		
	}
	public static void post(String path, Route route) {
		if (!flag) {
			flag = true;
			getInstance().start();
		}

		if (host == null) {
			routingTableWithoutHost.add(new RoutingTableEntry("POST", path, route, ""));	
		} else {
			routingTableWithHost.add(new RoutingTableEntry("POST", path, route, host));	
		}
	}
	
	// EC1: Multiple Hosts.
	public static void host(String host) {
		Server.host = host;
	}
	
	// EC3: filters
	public static void before(Filter filter) {
		if (!flag) {
			flag = true;
			getInstance().start();
		}
		beforeFilters.add(filter);
		
	}
	
	// EC3: filters
	public static void after(Filter filter) {
		if (!flag) {
			flag = true;
			getInstance().start();
		}
		afterFilters.add(filter);
	}
	
	// for HTTPS
	public static void securePort(int port) {
//		System.out.println("setting securePort as: " + port);
		Server.httpsPort = port;
	}

	
	public static class staticFiles
	{
		public static void location(String path) {
			getInstance().dir = path;
		}
	}






}
