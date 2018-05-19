
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Scanner;

/**
 * @author Carter Hart, Sahisnu Nimmakayalu, James Griffin
 *
 * Description: Participant of the coordinator pool. Sends and
 * recieves messages to and from the coordinator. Takes in one argument
 * and uses it to initialize the participant. Parameter taken in is
 * the name of the participant-config-file.txt
 *
 */
public class Participant {

	private int participantId;
    private String participantIp;
    private int coordPort;
    private String coordIp;
    private String logFileName;

    private static final int SUCCESS = 0;
    private static final int ERROR = -1;
    private static final int QUIT = -2;

    public static void main(String[] args) {

        boolean config = (args.length == 1);
        String configFile = config ? args[0] : "../configFiles/p1.txt";

        Participant participant = new Participant(configFile);
        participant.start();
    }

    /**
     * Parses the config file that is passed to the program by the user when
     * running Participant.java. Creates a participant object based on the
     * config-file contents. The first line is the participantId of participant, second
     * line is logfile name, and third line is IP address and port number of the
     * coordinator.
     *
     * @param fileName txt file of the participant configuration.
     * @throws FileNotFoundException if the config file cannot be found
     */
    private void parseConfigFile(String fileName) throws FileNotFoundException {

        // Error checking
        if (fileName == null || fileName.length() == 0) {
            System.out.println("Error: Expected a config file.");
            System.exit(0);
        }

        // Creates new file container for config file
        File file = new File(fileName);

        if (!file.exists() || file.isDirectory()) {
            System.out.println("Error : Invalid config file.");
            System.exit(0);
        }

        String inputAddress = null;
        try (Scanner partConfig = new Scanner(file)) {
            if (partConfig.hasNext()) {

                participantId = partConfig.nextInt();
                partConfig.nextLine();
                logFileName = partConfig.nextLine();
                inputAddress = partConfig.nextLine();

            } else {
                System.out.println("Error : Invalid config format.");
                System.exit(0);
            }

            // splits last line of file to split IP and Port
            String portStr;
            String[] addressArray = inputAddress.trim().split(" ");

            if (addressArray.length < 2) {
                coordIp = inputAddress;
                portStr = "5000";
            } else {
                coordIp = addressArray[0];
                portStr = addressArray[1];
            }

            // sets the port number for participant
            coordPort = Integer.valueOf(portStr);
        }

        File logFile = new File(logFileName);
        try {
            logFile.createNewFile();
        } catch (IOException e) {
            System.out.println("Error : Failed to create log file.");
        }
    }

    public Participant(String configFile) {

        try {
            parseConfigFile(configFile);
        } catch (FileNotFoundException e) {
            System.out.println("Error : Config file not found");
        }

        try {
            participantIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

	private void start() {
        CommandThread commandThread = new CommandThread();
        commandThread.start();
	}

    public class CommandThread extends Thread implements Runnable {

        private boolean registered;
        private boolean online;

        private Socket coordSock;
        private DataOutputStream out;
        private DataInputStream in;

        private CoordListener listenerThread;

        CommandThread() {

            online = false;
            registered = false;

            int attempts = 0;
            while (true) {

                // Establish a connection
                try {

                    // Get socket connection
                    coordSock = new Socket(coordIp, coordPort);
                    out = new DataOutputStream(coordSock.getOutputStream());
                    in = new DataInputStream(coordSock.getInputStream());

                    // Set participantId for Coordinator Thread
                    out.writeInt(participantId);

                    break;
                } catch (IOException e) {
                    // Errors expected, simply tries again 1 second later
                }

                if (attempts >= 120) {
                    System.out.println("Error : Failed to connect to Coordinator");
                    System.exit(0);
                }

                System.out.println("Unable to connect to coordinator... Trying again in 1 second");
                attempts++;

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void run() {

            Scanner in = new Scanner(System.in);
            String input;

            // Until break...
            boolean running = true;
            while (running) {

                System.out.print(participantId + " >> ");

                input = in.nextLine();

                String[] tokens = input.split(" ");

                if (tokens.length >= 1) {

                    String command = tokens[0].toLowerCase();

                    try {
                        out.writeUTF(command);
                        switch (command) {
                            case "register":
                                register(tokens);
                                break;
                            case "deregister":
                                deregister();
                                break;
                            case "disconnect":
                                disconnect();
                                break;
                            case "reconnect":
                                reconnect(tokens);
                                break;
                            case "msend":
                                msend(input);
                                break;
                            case "quit":
                            case "exit":
                                if (listenerThread != null) listenerThread.shutdown();
                                running = false;
                                break;
                            default:
                                System.out.println("Invalid command \"" + command + "\"");
                                break;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.out.println("\nError : Communication with coordinator failed.");
                    }
                }
            }

            in.close();
            shutdown();
            System.out.println("\nGoodbye.");
        }

        /**
         * Register
         * Participant must register with coordinator before being allowed to connect. The
         * participant must specify its participantId, IP Address, and the port of it's thread that will
         * receive multicast messages from the Coordinator. The listening thread must be operational
         * before sending the message to the coordinator. Upon successful registration, the
         * participant becomes a member of the multicast group and will begin receiving messages.
         *
         * @param args The arguments to register command
         */
        private void register(String[] args) throws IOException {

            if (registered) {
                System.out.println("Error : Must deregister before registering.");
                out.writeInt(ERROR);
                return;
            }

            if (args.length != 2) {
                System.out.println("Error : Expected a single port number argument.");
                out.writeInt(ERROR);
                return;
            }

            out.writeInt(SUCCESS);

            // out.writeInt(participantId);
            out.writeUTF(participantIp);
            out.writeInt(Integer.parseInt(args[1]));


            listenerThread = new CoordListener(logFileName, Integer.parseInt(args[1]));
            if (listenerThread.isRunning()) {
                listenerThread.start();
                registered = true;
                online = true;
                out.writeInt(SUCCESS);
            } else {
                out.writeInt(ERROR);
            }
        }

        /**
         * Deregister
         * Participant informs the Coordinator that it wishes to be removed from the multicast
         * group. Different than disconnecting. Participant will have to register again with the
         * Coordinator again. The Participant will also fail to get any messages that were sent
         * since it's deregistration. The listening thread of the Participant will die.
         */
        private void deregister() throws IOException {

            if (!registered) {
                System.out.println("Error : Must register before deregistering.");
                out.writeInt(ERROR);
                return;
            }
            out.writeInt(SUCCESS);

            online = false;
            registered = false;
            listenerThread.shutdown();
        }

        /**
         * Disconnect
         * Participant informs the Coordinator that it wishes to disconnect from the multicast group
         * and temporarily go offline. The Coordinator will queue up the messages sent to the Participant
         * while it is offline and will send them to it when the Participant reconnects. The listening thread
         * of the Participant will die.
         */
        private void disconnect() throws IOException {

            if (!registered) {
                System.out.println("Error : Must be registered to disconnect.");
                out.writeInt(ERROR);
                return;
            } else if (!online) {
                System.out.println("Error : Must be online to disconnect.");
                out.writeInt(ERROR);
                return;
            }
            out.writeInt(SUCCESS);

            online = false;
            listenerThread.shutdown();
        }

        /**
         * Reconnect
         * Participant notifies the Coordinator that they wish to reconnect to the multicast group. Lets
         * the Coordinator know that the Participant is now online. Participant specifies IP Address and
         * the port of where the Listener for the Participant will be. This listenerThread thread must be operational
         * before sending the message to the coordinator.
         *
         * @param args The arguments to reconnect command
         * @throws IOException Throws IOException if
         */
        private void reconnect(String[] args) throws IOException {

            if (args.length != 2) {
                System.out.println("Error : Expected a single port number argument.");
                out.writeInt(ERROR);
                return;
            } else if (!registered) {
                System.out.println("Error : Must be registered to connect.");
                out.writeInt(ERROR);
                return;
            } else if (online) {
                System.out.println("Error : Already connected.");
                out.writeInt(ERROR);
                return;
            }

            out.writeInt(SUCCESS);
            out.writeInt(Integer.parseInt(args[1]));

            // System.out.println("send data...");

            // Check if reconnect was successful
            int status = in.readInt();
            if (status != SUCCESS) {
                System.out.println("Error : Coordinator failed to reconnect.");
                return;
            }

            listenerThread = new CoordListener(logFileName, Integer.parseInt(args[1]));
            if (listenerThread.isRunning()) {
                listenerThread.start();
                online = true;
            }
        }

        /**
         * Multicast Send
         * Send a multicast message to all current members of the multicast group. The message sent is an
         * alpha-numeric string. The Participant sends the message to the coordinator, who then sends it to
         * everyone in the group.
         *
         * @param input The full command
         */
        private void msend(String input) throws IOException {

            String message;

            if (!registered) {
                System.out.println("Error : Participant is not registered, cannot send multicast.");
                out.writeInt(ERROR);
                return;
            } else if (!online) {
                System.out.println("Error : Participant is not online, cannot send multicast.");
                out.writeInt(ERROR);
                return;
            } else {
                int split = input.indexOf(' ');
                message = input.substring(split + 1);
                if (split == -1) {
                    out.writeInt(ERROR);
                    return;
                }
                out.writeInt(SUCCESS);
            }

            int status = in.readInt();
            if (status == ERROR) {
                System.out.println("Error : Participant is not registered");
                return;
            }

            out.writeUTF(message);

            status = in.readInt();
            if (status != SUCCESS) {
                System.out.println("Error : Coordinator failed to multicast.");
            }
        }

        private void shutdown() {
            // System.out.println("Shutting down participant.");
            if (listenerThread != null)
                listenerThread.shutdown();
            try {
                in.close();
                out.close();
                coordSock.close();
            } catch (IOException e) {
                //
            }
        }
    }


    /**
     * Listens to the Coordinator for multicast messages.
     */
    private class CoordListener extends Thread implements Runnable {

        private String fileName;
        private DataInputStream in;
        private ServerSocket socket;
        private Socket multicastSocket;
        private int listenPort;

        private boolean running;

        // private ArrayList<String> pendingMessages;

        CoordListener(String file, int port) {
            fileName = file;
            listenPort = port;
            running = true;
            listenForCoordinator();
        }

        @Override
        public void run() {
            try {

                RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
                file.seek(file.length());
                file.writeBytes("************************************************************\n");

                Date date;
                while (running) {

                    int status = in.readInt();
                    if (status != SUCCESS)
                        break;

                    String msg = in.readUTF();

                    date = new Date();
                    msg = date.toString() + " : " + msg + "\n";

                    file.writeBytes(msg);
                    file.seek(file.length());
                }
                file.seek(file.length());
                file.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        private void listenForCoordinator() {

            try {
                socket = new ServerSocket(listenPort);
            } catch (IOException e) {
                System.out.println("\nError : Could not create socket on port " + listenPort);
                running = false;
                return;
            } catch (SecurityException e) {
                System.out.println("\nError : (Security Exception) checkListen() failed.");
                running = false;
                return;
            } catch (IllegalArgumentException e) {
                System.out.println("\nError : Coordinator listening port " + listenPort + " is outside valid range.");
                running = false;
                return;
            }

            try {
                multicastSocket = socket.accept();
                in = new DataInputStream(multicastSocket.getInputStream());
            } catch (IOException e) {
                System.out.println("\nError : Error connecting to coordinator.");
                running = false;
            } catch (SecurityException e) {
                System.out.println("\nError : (Security Exception) Cannot accept incoming connection.");
                running = false;
            }
        }

        private void shutdown() {
            try {
                socket.close();
                running = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public boolean isRunning() {
            return running;
        }
    }
}
