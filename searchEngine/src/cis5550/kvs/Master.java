package cis5550.kvs;

import static cis5550.webserver.Server.*;

public class Master extends cis5550.generic.Master{
//	accept a single line command argument: the port number on which to run the server
	public static void main(String[] args) {
		if (args.length != 1){
			System.out.println("incorrect command");
			return;
		}
		int port = Integer.valueOf(args[0]);
		port(port);
		registerMasterRoutes();
		get("/", (request, response) -> "<html><head><title>KVS Master</title></head><body><h3>KVS Master</h3>\n" + workerTable() + "</body></html>");
	}
	
}
