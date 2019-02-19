
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RServer implements RInterface {
	
	private HashMap<Integer, ArrayList<String>> movieMap; //movieID, (title, genres)
	private HashMap<List<Integer>, ArrayList<String>> ratingsMap; //(userID, movieID), (rating, time stamp)
	//private HashMap<String, ArrayList<String>> linkMap; //movieID, (imdbID, tmdbID)
	//private ArrayList<ArrayList<String>> tagList; //userID, movieID, tag, time stamp
	private Status state;
	
	private static int noServers = 0;
	
	public RServer() {
		movieMap = new HashMap<Integer, ArrayList<String>>();
		ratingsMap = new HashMap<List<Integer>, ArrayList<String>>();
		//linkMap = new HashMap<String, ArrayList<String>>();
		//tagList = new ArrayList<ArrayList<String>>();
		state = Status.OK;
		noServers++;
		readFile("movies.csv", "movie");
		readFile("ratings.csv", "ratings");
		//readFile("links.csv", "links");
		//readFile("tags.csv", "tags");
	}
	
	public static void main(String args[]) {
		
		try {
		    // Create server object
		    RServer obj = new RServer();

		    // Create remote object stub from server object
		    RInterface stub = (RInterface) UnicastRemoteObject.exportObject(obj, 0);

		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000 * noServers);

		    // Bind the remote object's stub in the registry
		    String binding = "R" + Integer.toString(noServers);
		    registry.bind(binding, stub);

		    // Write ready message to console
		    System.err.println("Replicant " + binding + " ready at port " + Integer.toString(10000 * noServers));
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public void readFile(String fileName, String type) {
		String path = "../ml-latest-small/" + fileName;
		Path pathToFile = Paths.get(path);
		
		try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
			
			String line = br.readLine();
			line = br.readLine(); //first line is just the column names so we skip
			while (line != null) {
				if(type.equals("movie")) {
					addMovieEntry(line);
				}
				else if(type.equals("ratings")) {
					addRatingsEntry(line);
				}
				else if(type.equals("links")) {
					addLinksEntry(line);
				}
				else { //"tags"
					addTagEntry(line);
				}
				
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
		data.add(attr[1]);
		data.add(attr[2]);
		this.movieMap.put(Integer.valueOf(attr[0]), data);
	}
	
	public void addRatingsEntry(String line) {
		String[] attr = line.split(",");
		ArrayList<String> data = new ArrayList<>();
		int userID = Integer.valueOf(attr[0]);
		int movieID = Integer.valueOf(attr[1]);
		data.add(attr[2]);
		data.add(attr[3]);
		this.ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), data);
	}
	
	public void addLinksEntry(String line) {
		String[] attr = line.split(",");
		ArrayList<String> data = new ArrayList<>();
		data.add(attr[1]);
		data.add(attr[2]);
		//this.linkMap.put(attr[0], data);
	}
	
	public void addTagEntry(String line) {
		String[] attr = line.split(",");
		ArrayList<String> data = new ArrayList<>();
		data.add(attr[0]);
		data.add(attr[1]);
		data.add(attr[2]);
		data.add(attr[3]);
		//this.tagList.add(data);
	}
	
	public String getRating(int userID, int movieID) {
		try {
			ArrayList<String> movieData = movieMap.get(movieID);
			if(movieData == null) {
				return "Error:Specified movie does not exist.";
			}
			else {
				ArrayList<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					return "Error:This rating does not exist.";
				}
				else {
					return "Rating by " + Integer.toString(userID) + " for " + movieData.get(0) + " is " + rating.get(0) + " at timestamp " + rating.get(1);
				}
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
}

//rmiregistry <port number> on cmd before attempting to run.