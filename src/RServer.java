
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RServer implements RInterface {
	
	private HashMap<String, ArrayList<String>> movieSet; //Title, (movieID, genres)
	private HashMap<String, String> ratingsSet;
	private String state;
	
	private static int noServers = 0;
	
	public RServer() {
		movieSet = new HashMap<String, ArrayList<String>>();
		state = "OK";
		noServers++;
		readMovieFile("movies.csv", "movie");
		System.err.println(movieSet.get("Toy Story (1995)"));
	}
	
	public static void main(String args[]) {
		
		try {
		    // Create server object
		    RServer obj = new RServer();

		    // Create remote object stub from server object
		    RInterface stub = (RInterface) UnicastRemoteObject.exportObject(obj, 0);

		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);

		    // Bind the remote object's stub in the registry
		    String binding = "R" + Integer.toString(noServers);
		    registry.bind(binding, stub);

		    // Write ready message to console
		    System.err.println("Server ready");
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public void readMovieFile(String fileName, String type) {
		String path = "../ml-latest-small/" + fileName;
		Path pathToFile = Paths.get(path);
		
		try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
			
			String line = br.readLine();
			line = br.readLine(); //first line are just the column names so we skip
			while (line != null) {
				addMovieEntry(line);
				line = br.readLine();
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public void addMovieEntry(String line) {
		int i = line.indexOf(",");
		int j = line.lastIndexOf(",");
		String[] attr = {line.substring(0, i), line.substring(i+1, j), line.substring(j+1)};
		ArrayList<String> data = new ArrayList<>();
		data.add(attr[0]);
		data.add(attr[2]);
		this.movieSet.put(attr[1], data);
		System.out.println(movieSet.get(attr[1]));
	}
}

//rmiregistry <port number> on cmd before attempting to run.