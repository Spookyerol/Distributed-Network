import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface RInterface extends Remote {
	String[] getRating(int userID, int movieID, String[] checkVector) throws RemoteException;
	
	String[] submitRating(int userID, int movieID, String score, String[] checkVector) throws RemoteException;
	
	Status getStatus() throws RemoteException;
	
	HashMap<List<Integer>, List<String>> getRatings() throws RemoteException;
	
	int getID() throws RemoteException;
}
