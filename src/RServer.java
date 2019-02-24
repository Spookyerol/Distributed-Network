
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Date;
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
	private HashMap<List<Integer>, List<String>> ratingsMap; //(userID, movieID), (rating, time stamp)
	private int[] vectorStamp; //each cell points to a replicant that is in the network by index, the number in the cell represents the number of updates that this server thinks the corresponding replicants have
	private int id; //this index points to the cell in vectorStamp that corresponds to this object/server
	
	private static int noServers = 0;
	private static String[] replicants; 
	
	public RServer(int networkSize) {
		movieMap = new HashMap<Integer, ArrayList<String>>();
		ratingsMap = new HashMap<List<Integer>, List<String>>();
		id = noServers - 1;
		vectorStamp = new int[networkSize];
		readFile("movies.csv", "movie");
		readFile("ratings.csv", "ratings");
	}
	
	public static void main(String args[]) {
		
		try {
			int networkSize = Integer.valueOf(args[0]);
			String[] servers = new String[networkSize];
			
		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			for(int i = 0;i < networkSize;i++) {
				noServers++;
			    // Create server object
			    RServer obj = new RServer(networkSize);
			    // Create remote object stub from server object
			    RInterface stub = (RInterface) UnicastRemoteObject.exportObject(obj, 0);
	
			    // Bind the remote object's stub in the registry
			    String binding = "R" + Integer.toString(noServers);
			    registry.bind(binding, stub);
			    servers[i] = binding;
			    // Write ready message to console
			    System.err.println("Replicant " + binding + " ready at port " + Integer.toString(10000));
			}
			replicants = servers;
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
		int userID = Integer.valueOf(attr[0]);
		int movieID = Integer.valueOf(attr[1]);
		this.ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), Collections.unmodifiableList(Arrays.asList(attr[2], attr[3])));
	}
	
	public boolean checkForUpdates(int counter, int otherId) {
		if(this.vectorStamp[otherId] < counter) { //this means that the other replicant has more updates than this server expected, this trusts that the last used replica is up to date with all other replicas - since it served the previous request
		//note that this replicant could still have updates that the other one does not posess yet
			this.vectorStamp[otherId] = counter;
			return true; //need updates
		}
		else {
			return false; //no updates found
		}
	}
	
	public void applyAvailableUpdates(String[] checkVector) {
		try {
			boolean update = false;
			if(!checkVector[0].equals("FE")) {
				int otherId = Integer.valueOf(checkVector[0].substring(1)) - 1; //retrieves the id of the other replicant by separating out the 'R' from the numerical part
				update = checkForUpdates(Integer.valueOf(checkVector[1]), otherId);
			}
			if(update) {
				Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
				RInterface replicant = (RInterface) registry.lookup(checkVector[0]);
				while(replicant.getStatus() == Status.OFFLINE) { //since updating like this is high priority we still go through even if the server is overloaded
					wait(1); //We have to wait until the server is back up again
				}
				HashMap<List<Integer>, List<String>> otherMap = replicant.getRatings();
				for (Map.Entry<List<Integer>, List<String>> entry : otherMap.entrySet()) {
					if(this.ratingsMap.get(entry.getKey()) == null) { //there is an additional entry that must be added
						this.ratingsMap.put(entry.getKey(), entry.getValue());
						this.vectorStamp[id] = vectorStamp[id] + 1;
					}
					else if(this.ratingsMap.get(entry.getKey()) != entry.getValue()) { //a data point has been modified
						List<String> valuesSelf = this.ratingsMap.get(entry.getKey());
						long timeStampSelf = Long.parseLong(valuesSelf.get(1));
						//check to see which copy of data was most recently changed
						if(timeStampSelf < Long.parseLong(entry.getValue().get(1))) { //the other replicant has more recent data so we update
							this.ratingsMap.put(entry.getKey(), entry.getValue());
							this.vectorStamp[id] = vectorStamp[id] + 1;
						}
						//else we keep our copy of the data
					}
				}
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public String[] getRating(int userID, int movieID, String[] checkVector) {
		try {
			applyAvailableUpdates(checkVector);
			ArrayList<String> movieData = movieMap.get(movieID);
			if(movieData == null) {
				String msg = "Error:Specified movie does not exist.";
				String[] response = {msg, Integer.toString(vectorStamp[id]), replicants[id]};
				return response;
			}
			else {
				List<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					String msg = "Error:This rating does not exist.";
					String[] response = {msg, Integer.toString(vectorStamp[id]), replicants[id]};
					return response;
				}
				else {
					String msg = "Rating by " + Integer.toString(userID) + " for " + movieData.get(0) + " is " + rating.get(0) + " at time " + rating.get(1);
					String[] response = {msg, Integer.toString(vectorStamp[id]), replicants[id]};
					return response;
				}
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public String[] submitRating(int userID, int movieID, String score, String[] checkVector) {
		try {
			applyAvailableUpdates(checkVector);
			ArrayList<String> movieData = movieMap.get(movieID);
			if(movieData == null) {
				String msg = "Error:Specified movie does not exist.";
				String[] response = {msg, Integer.toString(vectorStamp[id]), replicants[id]};
				return response;
			}
			else {
				boolean update = true;
				List<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					update = false;
				}
				ArrayList<String> data = new ArrayList<String>();
				data.add(score);
				Date date = new Date();
				long timeStamp = date.getTime();
				String time = String.valueOf(timeStamp);
				data.add(time);
				ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), data);
				String msg;
				if(!update) {
					msg = "New rating by " + Integer.toString(userID) + " submitted for " + movieData.get(0) + " with score " + score + " at time " + time;
				}
				else {
					msg = "Existing rating for " + movieData.get(0) + " by " + Integer.toString(userID) + " updated to score " + score + " at time " + time;
				}
				this.vectorStamp[id] = vectorStamp[id] + 1;
				String[] response = {msg, Integer.toString(vectorStamp[id]), replicants[id]};
				return response;
			}
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public Status getStatus() {
		Status status = Status.OK;
		double p = Math.random();
		if(p < 0.05) {
			status = Status.OFFLINE;
		}
		else if(p < 0.3) {
			status = Status.OVERLOADED;
		}
		return status;
	}
	
	public HashMap<List<Integer>, List<String>> getRatings() {
		return this.ratingsMap;
	}
	
	public int getID() {
		return this.id;
	}
}



//rmiregistry <port number> on cmd before attempting to run.