package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.reflect.*;

import static cis5550.webserver.Server.*;
import cis5550.generic.Master;
import cis5550.kvs.KVSClient;
import cis5550.tools.*;

class FlameMaster extends Master {

  static int nextJobID = 1;
  static HashMap<String,String> outputs;
  public static KVSClient kvs;


	public static void main(String args[]) {

    // Check the command-line arguments
//		System.out.println("flamemaster main");
    if (args.length != 2) {
      System.err.println("Syntax: FlameMaster <port> <kvsMaster>");
      System.exit(1);
    }
   
    int myPort = Integer.valueOf(args[0]);
//    System.out.println("flameMaster myPort: " + myPort);
    kvs = new KVSClient(args[1]);
//    System.out.println("creating kvsClient in FlameMaster: " + kvs);
//    System.out.println(" kvsClient in FlameMaster: " + FlameMaster.kvs);
    outputs = new HashMap<String,String>();

  	port(myPort);
    registerMasterRoutes();

    /* Set up a little info page that can be used to see the list of registered workers */

    get("/", (request,response) -> {
      response.type("text/html");
      return "<html><head><title>Flame Master</title></head><body><h3>Flame Master</h3>\n" + workerTable() + "</body></html>";
    });

    /* Set up the main route for job submissions. This is invoked from FlameSubmit. */

    post("/submit", (request,response) -> {

      // Extract the parameters from the query string. The 'class' parameter, which contains the main class, is mandatory, and
      // we'll send a 400 Bad Request error if it isn't present. The 'arg1', 'arg2', ..., arguments contain command-line
      // arguments for the job and are optional.

    System.out.println("FlameMaster registering submit post route");
      String className = request.queryParams("class");
      System.out.println("FlameMaster register /submit className: " + className);
      if (className == null) {
        response.status(400, "Bad request");
        return "Missing class name (parameter 'class')";
      }
      Vector<String> argVector = new Vector<String>();
      for (int i=1; request.queryParams("arg"+i) != null; i++) 
        argVector.add(URLDecoder.decode(request.queryParams("arg"+i), "UTF-8"));
//      System.out.println("FlameMaster register /submit argVector: " + argVector);
      outputs.remove(Thread.currentThread().getName());

      // We begin by uploading the JAR to each of the workers. This should be done in parallel, so we'll use a separate
      // thread for each upload.

      Vector<String> workers = getWorkers();
//      System.out.println("getWorkers: " + workers);
      Thread threads[] = new Thread[getWorkers().size()];
      String results[] = new String[getWorkers().size()];
      for (int i=0; i<getWorkers().size(); i++) {
        final String url = "http://"+getWorkers().elementAt(i)+"/useJAR";
        final int j = i;
        threads[i] = new Thread("JAR upload #"+(i+1)) {
          public void run() {
            try {
              results[j] = new String(HTTP.doRequest("POST", url, request.bodyAsBytes()).body());
            } catch (Exception e) {
              results[j] = "Exception: "+e;
              e.printStackTrace();
            }
          }
        };
        threads[i].start();
      }

      // Wait for all the uploads to finish

      for (int i=0; i<threads.length; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException ie) {
        }
      }

      // Write the JAR file to a local file. Remember, we will need to invoke the 'run' method of the job, but, if the job
      // is submitted from a machine other than the master, the master won't have a local copy of the JAR file. We'll use
      // a different file name each time.

      int id = nextJobID++;
      String jarName = "job-"+id+".jar";
      File jarFile = new File(jarName);
      FileOutputStream fos = new FileOutputStream(jarFile);
      fos.write(request.bodyAsBytes());
      fos.close();

      // Load the class whose name the user has specified with the 'class' parameter, find its 'run' method, and 
      // invoke it. The parameters are 1) an instance of a FlameContext, and 2) the command-line arguments that
      // were provided in the query string above, if any. Several things can go wrong here: the class might not 
      // exist or might not have a run() method, and the method might throw an exception while it is running,
      // in which case we'll get an InvocationTargetException. We'll extract the underlying cause and report it
      // back to the user in the HTTP response, to help with debugging.

      FlameContextImpl flameContext = new FlameContextImpl(jarName, kvs);
      
      try {
//    	  System.out.println("FlameMaster try invokeRunMethod()");
        Loader.invokeRunMethod(jarFile, className, flameContext, argVector);
        response.body(flameContext.getOutput());
        return flameContext.getOutput();
      } catch (IllegalAccessException iae) {
        response.status(400, "Bad request");
        return "Double-check that the class "+className+" contains a public static run(FlameContext, String[]) method, and that the class itself is public!";
      } catch (NoSuchMethodException iae) {
        response.status(400, "Bad request");
        return "Double-check that the class "+className+" contains a public static run(FlameContext, String[]) method";
      } catch (InvocationTargetException ite) {
        ite.printStackTrace();
        StringWriter sw = new StringWriter();
        ite.getCause().printStackTrace(new PrintWriter(sw));
        response.status(500, "Job threw an exception");
        return sw.toString();
      }

//      return "Job finished, but produced no output";
    });

    get("/version", (request, response) -> { return "v1.2 Oct 28 2022"; });
	}
}