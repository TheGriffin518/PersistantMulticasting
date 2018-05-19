/**
 * @author Carter Hart, Sahisnu Nimmakayalu, James Griffin
 *
 * Description: Runs with config file that determines the port. Allows
 * participants to connect and register, send messages, multi-cast messages to
 * all of the members of the pool.
 */

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

public class Coordinator {

	private ServerSocket coordinatorSock;
    private int port;
    private int threshold;

	private Hashtable<Integer, PStub> members;
	private Hashtable<Integer, Queue<Message>> pendingMessages;

	private final Object membersMutex = new Object();
    private final Object pendingMutex = new Object();

    private static final int SUCCESS = 0;
    private static final int ERROR = -1;
    private static final int QUIT = -2;

    public static void main (String args[]){
        if (args.length != 1) {
            System.out.println("Error : Expected single config file path argument.");
            System.exit(0);
        }

        Coordinator coord = new Coordinator(args[0]);
        coord.run();
    }

	public Coordinator(String configFile) {
		this.members = new Hashtable<>();
		this.pendingMessages = new Hashtable<>();

        try {
            parseConfigFile(configFile);
        } catch (FileNotFoundException e) {
            System.out.println("Error : Config file not found");
        }
	}

    private void parseConfigFile(String fileName) throws FileNotFoundException {
        File configFile = new File(fileName);

        try {
            Scanner coordConfig = new Scanner(configFile);
            port = Integer.parseInt(coordConfig.nextLine());
            threshold = Integer.parseInt(coordConfig.nextLine()) * 1000; // Convert to milliseconds

            // System.out.println("Threshold to save messages is " + threshold + " milliseconds.");
        } catch (IOException e) {
            System.out.println("Error : Failed to parse config file.");
        }
    }

	/**
	 * Creates a new ServerSocket and then listens for a new connection from a participant.
	 */
	private void run()  {

		// creates new serversocket on this port. Then listens for incoming connections.
		// Only will accept up to 20 connections, based on backlog parameet
		// er
		try {

            coordinatorSock = new ServerSocket(port, 20);

            // Listens for a new participant
			listen();

		} catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                coordinatorSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
		}
	}

	/**
	 * Listens for a new connection from a participant. Runs indefinitely as long as the
	 * coordinator is running.
	 * @throws IOException
	 */
	private void listen()  {

	    // Needs SOME break condition right?
        System.out.println("Waiting for any participant on port " + port + "...");
        while (true){
            try {
                Socket clientSock = coordinatorSock.accept();

                CoordinatorThread coordThread = new CoordinatorThread(clientSock, threshold);
                coordThread.start();

            } catch (IOException e) {
		        e.printStackTrace();
            }
		}
	}

    public class CoordinatorThread extends Thread {

        private Socket clientSock;
        private DataInputStream in;
        private DataOutputStream out;
        private int timeout;

        private int participantId;

        CoordinatorThread(Socket clientSock, int timeout) {
            this.clientSock = clientSock;
            this.timeout = timeout;

            try {

                // the outgoing message buffer from coordinator to participant
                out = new DataOutputStream(clientSock.getOutputStream());

                // the incoming message buffer from participant to be read by coordinator
                in = new DataInputStream(clientSock.getInputStream());

                participantId = in.readInt();
                System.out.println("Connected with participant " + participantId);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            String input;
            boolean running = true;

            while (running) {
                try {
                    // Read command from the participant
                    // System.out.println("Waiting for user input...");
                    input = in.readUTF();

                    System.out.println("Participant " + participantId + " >> " + input);

                    switch (input) {
                        case "register":
                            register();
                            break;
                        case "deregister":
                            deregister();
                            break;
                        case "disconnect":
                            disconnect();
                            break;
                        case "reconnect":
                            reconnect();
                            break;
                        case "msend":
                            msend();
                            break;
                        case "exit":
                        case "quit":
                            running = false;
                            quit();
                            break;
                        default:
                            System.out.println("Error : Unrecognized Command");
                            break;
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                in.close();
                out.close();
                clientSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Registers the new participant with the coordinatorThread. Places the id of the
         * participant into a data structure that the coordinator references to determine
         * if a participant has connected before or not.
         */
        private void register() throws IOException {

            int status = in.readInt();
            if (status != SUCCESS)
                return;

            String ip = in.readUTF();
            int port = in.readInt();

            // Create a new participant
            PStub participant = new PStub(participantId, ip, port, true);

            status = in.readInt();

            // Add new participant to the group.
            if (status == SUCCESS) {
                synchronized (membersMutex) {
                    members.put(participantId, participant);
                }
                synchronized (pendingMutex) {
                    pendingMessages.put(participantId, new LinkedList<>());
                }
            }
        }

        /**
         * Deregisters the participant from the coordinator. Removes the participant's id from
         * the data structure that the coordinator references to determine if participant has
         * connected before.
         */
        private void deregister() throws IOException {

            int status = in.readInt();
            if (status != SUCCESS)
                return;

            synchronized (membersMutex) {
                quitParticipantListener(members.get(participantId));
                members.remove(participantId);
            }

            synchronized (pendingMutex) {
                pendingMessages.remove(participantId);
            }
        }

        /**
         * Disconnects the current participant from the coordinator. The coordinator
         * then knows to queue up messages for the participant.
         */
        private void disconnect() throws IOException {

            int status = in.readInt();
            if (status != SUCCESS)
                return;

            synchronized (membersMutex) {
                PStub participant = members.get(participantId);
                quitParticipantListener(participant);
                participant.setOnline(false);
            }
        }

        /**
         * Reconnects the participant to the coordinator. Lets the participant know that
         * they have already registered.
         * @throws IOException 
         */
        private void reconnect() throws IOException {

            int status = in.readInt();
            if (status != SUCCESS)
                return;

            int port = in.readInt();

            PStub participant;
            synchronized (membersMutex) {
                participant = members.get(participantId);
                if (participant == null) {
                    System.out.println("Error : Participant was not registered.");
                    out.writeInt(ERROR);
                    return;
                } else {
                    out.writeInt(SUCCESS);
                    participant.setOnline(true);
                    participant.setRecieveingPort(port);
                    participant.connect();
                }
            }

            synchronized (pendingMutex) {
                Queue<Message> received = pendingMessages.get(participantId);
                if (received != null) {
                    while (received.size() > 0) {
                        Message message = received.peek();
                        long elapsed = System.currentTimeMillis() - message.getTimestamp();
                        if (elapsed < timeout) {
                            sendParticipantMessage(participant, message.getMessage());
                        }
                        received.remove();
                    }
                }
            }
        }

        /**
         * Sends a message to all memebers of the multicast group.
         */
        private void msend() throws IOException {

            int status = in.readInt();
            if (status != SUCCESS)
                return;

            // Check if ID is registered
            synchronized (membersMutex) {
                if (members.get(participantId) == null) {
                    out.writeInt(ERROR);
                    return;
                } else
                    out.writeInt(SUCCESS);
            }

            String message = in.readUTF();

            synchronized (membersMutex) {
                PStub participant;
                for (Integer id : members.keySet()) {
                    participant = members.get(id);
                    if (participant.isOnline()) {
                        sendParticipantMessage(participant, message);
                    } else {
                        synchronized (pendingMutex) {
                            Message msg = new Message(message);
                            pendingMessages.get(participant.getId()).add(msg);
                        }
                    }
                }
            }

            out.writeInt(SUCCESS);
        }

        private void quitParticipantListener(PStub participant) throws IOException {
            if (participant == null)
                return;
            participant.writeInt(QUIT);
        }

        private void sendParticipantMessage(PStub participant, String msg) throws IOException {
            if (participant == null)
                return;
            participant.writeInt(SUCCESS);
            participant.writeUTF(msg);
        }

        private void quit() throws IOException {

            synchronized (membersMutex) {
                quitParticipantListener(members.get(participantId));
                members.remove(participantId);
            }

            synchronized (pendingMutex) {
                pendingMessages.remove(participantId);
            }
        }
    }

    class PStub {

	    private int id;
	    private String ipAddress;
	    private int receivingPort;
	    private boolean online;

	    private Socket partSocket;
	    private DataInputStream in;
        private DataOutputStream out;

        // private long timeSinceOnline;

        PStub(int id, String ipAddress, int recievingPort, boolean online) {
            this.id = id;
            this.ipAddress = ipAddress;
            this.receivingPort = recievingPort;
            this.online = online;
            connect();
        }

        public void setRecieveingPort(int port) {
            receivingPort = port;
        }

        public void setOnline(boolean b) {
	        online = b;
        }

        public int getId() {
	        return id;
        }

        public String getIpAddres() {
            return ipAddress;
        }

        public int getRecieveingPort() {
	        return receivingPort;
        }

        public boolean isOnline() {
	        return online;
        }

        public String readUTF() throws IOException {
            return in.readUTF();
        }

        public void writeUTF(String msg) throws IOException {
            out.writeUTF(msg);
        }

        public void writeInt(int code) throws IOException {
            out.writeInt(code);
        }

        public void connect() {

            // System.out.println("Connecting to participant " + ipAddress + " " + receivingPort);

            int attempts = 0;
            while (true) {
                // Establish a connection
                try {
                    // Get socket connection
                    partSocket = new Socket(ipAddress, receivingPort);
                    out = new DataOutputStream(partSocket.getOutputStream());
                    in = new DataInputStream(partSocket.getInputStream());
                    break;
                } catch (IOException e) {
                    // Errors expected, simply tries again 1 second later
                }

                if (attempts >= 10) {
                    System.out.println("Coordinator Failed to connect to Participant");
                    online = false;
                    return;
                }

                // System.out.println("Unable to connect to participant... Trying again in 1 second");
                attempts++;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // System.out.println("Successfully connected to participant " + ipAddress + ":" + receivingPort);
        }

        public void disconnect() {
            try {
                partSocket.close();
            } catch (IOException e) {
                //
            }
        }
    }

    class Message {

	    String message;
	    long timestamp;

	    Message(String msg) {
	        message = msg;
	        timestamp = System.currentTimeMillis();
        }

        public String getMessage() {
            return message;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
