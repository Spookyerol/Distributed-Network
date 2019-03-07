
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
	
	private HashMap<Integer, ArrayList<String>> movieMap; //stored as movieID, (title, genres)
	private HashMap<List<Integer>, List<String>> ratingsMap; //stored as (userID, movieID), (rating, time stamp)
	private int[] updateCounters; //each cell points to a replica that is in the network by index, the number in the cell represents the number of updates that this server thinks the corresponding replicas have
	private int id; //this index points to the cell in updateCounters that corresponds to this object/server
	private String[] bindingList;
	
	public RServer(int networkSize) {
		movieMap = new HashMap<Integer, ArrayList<String>>();
		ratingsMap = new HashMap<List<Integer>, List<String>>();
		updateCounters = new int[networkSize];
		readFile("movies.csv", "movie");
		readFile("ratings.csv", "ratings");
	}
	
	// Main function that does all the initialization and starts the system
	public static void main(String args[]) {
		
		try {
			int networkSize = Integer.valueOf(args[0]);
			String[] bindings = new String[networkSize];
			RServer[] servers = new RServer[networkSize];
						
		    Registry registry = LocateRegistry.createRegistry(10000);
			for(int i = 0;i < networkSize;i++) {
				
			    RServer obj = new RServer(networkSize);
			    servers[i] = obj;
			    
			    RInterface stub = (RInterface) UnicastRemoteObject.exportObject(obj, 0);
	
			    String binding = "R" + Integer.toString(i+1);
			    registry.bind(binding, stub);
			    bindings[i] = binding;
			    obj.setId(i);
			    
			    System.err.println("replica " + binding + " ready at port " + Integer.toString(10000));
			}
			
			for(int i = 0;i < bindings.length;i++) {
				servers[i].setAvailBindings(bindings);
			}
		} catch (Exception e) {
		    System.err.println("Replica exception: " + e.toString());
		    e.printStackTrace();
		}
		
		// This is the background thread which runs and wraps the gossip method
		Runnable r = new Runnable() {
	         public void run() {
	        	 try {
	        		 while(true) { // Keep looping through in the background
		        		 Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
		        		 String[] servers = registry.list();
		        		 int noServers = servers.length;
		        		 // Since the front end can be on the same registry as the replicas we make sure that we don't involve it in the gossip
		        		 for(int i=0; i < registry.list().length;i++) {
		        			 if(servers[i].equals("FE")) {
		        				 noServers--;
		        			 }
		        		 }
		        		 // Each replica will report it's status
		        		 Status[] states = new Status[noServers];
		        		 for(int i=0;i < noServers;i++) {
		        			 String binding = "R" + Integer.toString(i+1);
		        			 RInterface replica = (RInterface) registry.lookup(binding);
		        			 states[i] = replica.getStatus();
		        			 
		        		 }
		        		 // Those that are available will gossip amongst each other
		        		 for(int i=0;i < noServers;i++) {
		        			 if(states[i] == Status.OK) {
		        				 String binding = "R" + Integer.toString(i+1);
		        				 RInterface replica = (RInterface) registry.lookup(binding);
		        				 replica.gossipForUpdates(registry, states);
		        			 }
		        		 }
	        			 System.out.println("Successfully gossiped.");
	        			 Thread.sleep(10000); // Wait for arg / 1000 seconds
	        		 }
	        	 } catch (Exception e) {
	     		    System.err.println("Gossip exception: " + e.toString());
	    		    e.printStackTrace();
	    		}
	         }
	    };

	    new Thread(r).start(); // This line is async - will not wait for the task in runnable to finish
	}
	
	// Function that can read the data on both movies.csv and ratings.csv
	public void readFile(String fileName, String type) {
		String path = "../ml-latest-small/" + fileName;
		Path pathToFile = Paths.get(path);
		
		try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
			
			String line = br.readLine();
			line = br.readLine(); // First line is just the column names so we skip
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
	
	// Adds data from a line of movies.csv to the servers data set
	public void addMovieEntry(String line) {
		int i = line.indexOf(",");
		int j = line.lastIndexOf(",");
		String[] attr = {line.substring(0, i), line.substring(i+1, j), line.substring(j+1)};
		ArrayList<String> data = new ArrayList<>();
		data.add(attr[1]);
		data.add(attr[2]);
		this.movieMap.put(Integer.valueOf(attr[0]), data);
	}
	
	// Adds data from a line of ratings.csv to the servers data set
	public void addRatingsEntry(String line) {
		String[] attr = line.split(",");
		int userID = Integer.valueOf(attr[0]);
		int movieID = Integer.valueOf(attr[1]);
		this.ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), Collections.unmodifiableList(Arrays.asList(attr[2], attr[3])));
	}
	
	// Small function that takes int counter holding the number of updates another replica has and int otherId which is the id of the other replica to decide whether or not this replica has missed an update
	public boolean checkForUpdates(int counter, int otherId) {
		if(this.updateCounters[otherId] < counter) { // This means that the other replica has more updates than this server expected, this trusts that the last used replica is up to date with all other replicas - since it served the previous request
		// Note that this replica could still have updates that the other one does not possess yet
			return true; //need updates
		}
		else {
			return false; //no updates found
		}
	}
	
	// Function designed to run in the background and sync up all replicas with eachother. Takes the registry and an array of server states
	public void gossipForUpdates(Registry registry, Status[] states) {
		try {
			for(int i=0;i < bindingList.length;i++) {
				RInterface replica = (RInterface) registry.lookup(bindingList[i]);
				if(!replica.equals(this)) {
					if(states[replica.getId()] == Status.OK) {
						if(checkForUpdates(this.updateCounters[replica.getId()], replica.getId())) {
							this.applyUpdates(replica);
							this.updateCounters[replica.getId()] = replica.getUpdateCounters()[replica.getId()];
						}
					}
					// We cannot gossip with a replica that is not available
				}
			}
		} catch (Exception e) {
		    System.err.println("Replica exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	// Function that takes data about the lastVisited replica and uses that to find and apply any updates to this replica
	public void getAvailableUpdates(String[] lastVisited) {
		try {
			int updatesToGet = Integer.valueOf(lastVisited[1]);
			String updateReplica = lastVisited[0];
			Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			boolean update = false;
			if(!lastVisited[0].equals("FE")) {
				int otherId = Integer.valueOf(updateReplica.substring(1)) - 1; // Retrieves the id of the other replica by separating out the 'R' from the numerical part
				update = checkForUpdates(updatesToGet, otherId);
			}
			else {
				String[] servers = registry.list();
				int noServers = servers.length - 1;
	       		for(int i=0;i < noServers;i++) {
	       			String binding = "R" + Integer.toString(i+1);
	       			RInterface replica = (RInterface) registry.lookup(binding);
	       			if(!replica.equals(this)) {
		       			while(replica.getStatus() != Status.OK) { // This is called when an update is needed for a request so we must wait until an update arrives
		       				Thread.sleep(100); //We have to keep trying until the server is back up again
						}
	       				updatesToGet = replica.getUpdateCounters()[replica.getId()];
	       				if(checkForUpdates(updatesToGet, i)) {
		       				applyUpdates(replica);
		       				this.updateCounters[replica.getId()] = updatesToGet;
	       				}
	       			}
	       		}
			}
			if(update) {
				RInterface replica = (RInterface) registry.lookup(updateReplica);
				while(replica.getStatus() != Status.OK) { // This is called when an update is needed for a request so we must wait until an update arrives
					Thread.sleep(100); //We have to wait until the server is back up again
				}
				applyUpdates(replica);
				this.updateCounters[replica.getId()] = updatesToGet;
				
			}
		} catch (Exception e) {
		    System.err.println("Replica exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	// Function that takes another replica and applies its updates to the replica object it belongs to
	public void applyUpdates(RInterface replica) {
		try {
			HashMap<List<Integer>, List<String>> otherMap = replica.getRatings();
			for (Map.Entry<List<Integer>, List<String>> entry : otherMap.entrySet()) {
				if(this.ratingsMap.get(entry.getKey()) == null) { // There is an additional entry that must be added
					this.ratingsMap.put(entry.getKey(), entry.getValue());
					this.updateCounters[id] = updateCounters[id] + 1;
				}
				else if(this.ratingsMap.get(entry.getKey()) != entry.getValue()) { // A data point has been modified
					List<String> valuesSelf = this.ratingsMap.get(entry.getKey());
					long timeStampSelf = Long.parseLong(valuesSelf.get(1));
					// Conflicting update so check to see which copy of data was most recently changed
					if(timeStampSelf < Long.parseLong(entry.getValue().get(1))) { // The other replica has more recent data so we update
						this.ratingsMap.put(entry.getKey(), entry.getValue());
						this.updateCounters[id] = updateCounters[id] + 1;
					}
					//else we keep our copy of the data
				}
			}
		} catch (Exception e) {
		    System.err.println("Replica exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	// Function that handles queries with user and movie id and the last visited replica's update information in string array lastVisited
	public String[] getRating(int userID, int movieID, String[] lastVisited) {
		try {
			getAvailableUpdates(lastVisited); // We need to check if the data has been updated since the last gossip
			// Check if movie exists
			ArrayList<String> movieData = movieMap.get(movieID);
			if(movieData == null) {
				String msg = "Error:Specified movie does not exist.";
				String[] response = {msg, Integer.toString(updateCounters[id]), bindingList[id]};
				return response;
			}
			else {
				// Check if rating exists
				List<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					String msg = "Error:This rating does not exist.";
					String[] response = {msg, Integer.toString(updateCounters[id]), bindingList[id]};
					return response;
				}
				else {
					// Rating found
					String msg = "Rating by " + Integer.toString(userID) + " for " + movieData.get(0) + " is " + rating.get(0) + " at time " + rating.get(1);
					String[] response = {msg, Integer.toString(updateCounters[id]), bindingList[id]};
					return response;
				}
			}
		} catch (Exception e) {
		    System.err.println("Replica exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	// Function to handle data submission which accepts user and movie id as ints and score as string
	public String[] submitRating(int userID, int movieID, String score) {
		try {
			// Unlike for queries we do not need to check other replicas for updates since those changes are resolved during gossip
			ArrayList<String> movieData = movieMap.get(movieID);
			// Check if movie exits
			if(movieData == null) {
				String msg = "Error:Specified movie does not exist.";
				String[] response = {msg, Integer.toString(updateCounters[id]), bindingList[id]};
				return response;
			}
			else {
				// Check if rating exists to determine whether or not the change is an update or submission
				boolean update = true; // Assume it is an update
				List<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					update = false; // Is a new submission
				}
				ArrayList<String> data = new ArrayList<String>();
				data.add(score);
				// Set a timestamp for when this data was added to the database
				Date date = new Date();
				long timeStamp = date.getTime();
				String time = String.valueOf(timeStamp);
				data.add(time);
				ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), data);
				// Set appropriate response message
				String msg;
				if(!update) {
					msg = "New rating by user " + Integer.toString(userID) + " submitted for " + movieData.get(0) + " with score " + score + " at time " + time;
				}
				else {
					msg = "Existing rating for " + movieData.get(0) + " by user " + Integer.toString(userID) + " updated to score " + score + " at time " + time;
				}
				this.updateCounters[id] = updateCounters[id] + 1;
				String[] response = {msg, Integer.toString(updateCounters[id]), bindingList[id]};
				return response;
			}
		} catch (Exception e) {
			System.err.println("Replica exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	// Arbitrarily sets the status of the server
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
	
	// Basic mutation and retrieval functions
	
	public HashMap<List<Integer>, List<String>> getRatings() {
		return this.ratingsMap;
	}
	
	public int getId() {
		return this.id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public void setAvailBindings(String[] bindings) {
		this.bindingList = bindings;
	}
	
	public int[] getUpdateCounters() {
		return this.updateCounters;
	}
}