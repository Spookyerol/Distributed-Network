import java.rmi.Remote;
import java.rmi.RemoteException;

public interface FEInterface extends Remote {
	String testRemote() throws RemoteException;
}
