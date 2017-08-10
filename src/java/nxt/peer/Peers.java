package nxt.peer;

import nxt.*;
import nxt.db.Db;
import nxt.util.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.DoSFilter;
import org.eclipse.jetty.servlets.GzipFilter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

public final class Peers {

    private static final Logger logger = LoggerFactory.getLogger(Peers.class);

    public static enum Event {
        BLACKLIST, UNBLACKLIST, DEACTIVATE, REMOVE,
        DOWNLOADED_VOLUME, UPLOADED_VOLUME, WEIGHT,
        ADDED_ACTIVE_PEER, CHANGED_ACTIVE_PEER,
        NEW_PEER
    }

    static final int LOGGING_MASK_EXCEPTIONS = 1;
    static final int LOGGING_MASK_NON200_RESPONSES = 2;
    static final int LOGGING_MASK_200_RESPONSES = 4;
    static final int communicationLoggingMask;

    static final Set<String> wellKnownPeers;
    static final Set<String> knownBlacklistedPeers;

    private static final int connectWellKnownFirst;
    private static boolean connectWellKnownFinished;

    static final Set<String> rebroadcastPeers;
    private static final Map<String, Integer> priorityBlockFeeders = new ConcurrentHashMap<>();
    private static final int priorityFeederInterval = Nxt.getIntProperty("burst.priorityFeederInterval") != 0 ? Nxt.getIntProperty("burst.priorityFeederInterval") : 300;

    static final int connectTimeout;
    static final int readTimeout;
    static final int blacklistingPeriod;
    static final boolean getMorePeers;
    static final int MAX_REQUEST_SIZE = 1024 * 1024;
    static final int MAX_RESPONSE_SIZE = 1024 * 1024;
    static final boolean useWebSockets;
    static final int webSocketIdleTimeout;
    static final boolean useProxy = System.getProperty("socksProxyHost") != null || System.getProperty("http.proxyHost") != null;

    static final int DEFAULT_PEER_PORT = 8123;
    static final int TESTNET_PEER_PORT = 7123;
    private static final String myPlatform;
    private static final String myAddress;
    private static final int myPeerServerPort;
    private static final String myHallmark;
    private static final boolean shareMyAddress;
    private static final int maxNumberOfConnectedPublicPeers;
    private static final boolean enableHallmarkProtection;
    private static final int pushThreshold;
    private static final int pullThreshold;
    private static final int sendToPeersLimit;
    private static final boolean usePeersDb;
    private static final boolean savePeers;
    static final boolean ignorePeerAnnouncedAddress;
    private static final String dumpPeersVersion;
    static final boolean cjdnsOnly;
    static final int MAX_VERSION_LENGTH = 10;
    static final int MAX_APPLICATION_LENGTH = 20;
    static final int MAX_PLATFORM_LENGTH = 30;
    static final int MAX_ANNOUNCED_ADDRESS_LENGTH = 100;


    static final JSONStreamAware myPeerInfoRequest;
    static final JSONStreamAware myPeerInfoResponse;

    private static final Listeners<Peer,Event> listeners = new Listeners<>();

    private static final ConcurrentMap<String, PeerImpl> peers = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> announcedAddresses = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> selfAnnouncedAddresses = new ConcurrentHashMap<>();

    static final Collection<PeerImpl> allPeers = Collections.unmodifiableCollection(peers.values());

    private static final ExecutorService sendToPeersService = Executors.newCachedThreadPool();
    private static final ExecutorService sendingService = Executors.newFixedThreadPool(10);


    static {

        myPlatform = Nxt.getStringProperty("nxt.myPlatform");
        if (myPlatform.length() > MAX_PLATFORM_LENGTH) {
            throw new RuntimeException("nxt.myPlatform length exceeds " + MAX_PLATFORM_LENGTH);
        }
        myAddress = Nxt.getStringProperty("nxt.myAddress");
        if (myAddress != null && myAddress.endsWith(":" + TESTNET_PEER_PORT) && !Constants.isTestnet) {
            throw new RuntimeException("Port " + TESTNET_PEER_PORT + " should only be used for testnet!!!");
        }
        myPeerServerPort = Nxt.getIntProperty("nxt.peerServerPort");
        if (myPeerServerPort == TESTNET_PEER_PORT && !Constants.isTestnet) {
            throw new RuntimeException("Port " + TESTNET_PEER_PORT + " should only be used for testnet!!!");
        }
        shareMyAddress = Nxt.getBooleanProperty("nxt.shareMyAddress") && ! Constants.isOffline;
        myHallmark = Nxt.getStringProperty("nxt.myHallmark");
        if (Peers.myHallmark != null && Peers.myHallmark.length() > 0) {
            try {
                Hallmark hallmark = Hallmark.parseHallmark(Peers.myHallmark);
                if (!hallmark.isValid() || myAddress == null) {
                    throw new RuntimeException();
                }
                URI uri = new URI("http://" + myAddress.trim());
                String host = uri.getHost();
                if (!hallmark.getHost().equals(host)) {
                    throw new RuntimeException();
                }
            } catch (RuntimeException | URISyntaxException e) {
                logger.info("Your hallmark is invalid: " + Peers.myHallmark + " for your address: " + myAddress);
                throw new RuntimeException(e.toString(), e);
            }
        }

        JSONObject json = new JSONObject();
        if (myAddress != null && myAddress.length() > 0) {
            try {
                URI uri = new URI("http://" + myAddress.trim());
                String host = uri.getHost();
                int port = uri.getPort();
                String announcedAddress;
                if (!Constants.isTestnet) {
                    if (port >= 0)
                        announcedAddress = myAddress;
                    else
                        announcedAddress = host + (myPeerServerPort != DEFAULT_PEER_PORT ? ":" + myPeerServerPort : "");
                } else {
                    announcedAddress = host;
                }
                if (announcedAddress == null || announcedAddress.length() > MAX_ANNOUNCED_ADDRESS_LENGTH) {
                    throw new RuntimeException("Invalid announced address length: " + announcedAddress);
                }
                json.put("announcedAddress", announcedAddress);
            } catch (URISyntaxException e) {
                logger.info("Your announce address is invalid: " + myAddress);
                throw new RuntimeException(e.toString(), e);
            }
        }
        if (Peers.myHallmark != null && Peers.myHallmark.length() > 0) {
            json.put("hallmark", Peers.myHallmark);
        }
        json.put("application", Nxt.APPLICATION);
        json.put("version", Nxt.VERSION);
        json.put("platform", Peers.myPlatform);
        json.put("shareAddress", Peers.shareMyAddress);
        logger.debug("My peer info:\n" + json.toJSONString());
        myPeerInfoResponse = JSON.prepare(json);
        json.put("requestType", "getInfo");
        myPeerInfoRequest = JSON.prepareRequest(json);

        rebroadcastPeers = Collections.unmodifiableSet(new HashSet<>(Nxt.getStringListProperty("burst.rebroadcastPeers")));

        List<String> priorityFeederAddresses = Nxt.getStringListProperty("burst.priorityBlockFeeders");
        for(String address : priorityFeederAddresses) {
            priorityBlockFeeders.put(address, 0);
        }

        List<String> wellKnownPeersList = Constants.isTestnet ? Nxt.getStringListProperty("nxt.testnetPeers")
                : Nxt.getStringListProperty("nxt.wellKnownPeers");
        for(String rePeer : rebroadcastPeers) {
            if(!wellKnownPeersList.contains(rePeer)) {
                wellKnownPeersList.add(rePeer);
            }
        }
        for(String pPeer : priorityFeederAddresses) {
            if(!wellKnownPeersList.contains(pPeer)) {
                wellKnownPeersList.add(pPeer);
            }
        }

        if (wellKnownPeersList.isEmpty() || Constants.isOffline) {
            wellKnownPeers = Collections.emptySet();
        } else {
            wellKnownPeers = Collections.unmodifiableSet(new HashSet<>(wellKnownPeersList));
        }

        connectWellKnownFirst = Nxt.getIntProperty("burst.connectWellKnownFirst");
        connectWellKnownFinished = (connectWellKnownFirst == 0);

        List<String> knownBlacklistedPeersList = Nxt.getStringListProperty("nxt.knownBlacklistedPeers");
        if (knownBlacklistedPeersList.isEmpty()) {
            knownBlacklistedPeers = Collections.emptySet();
        } else {
            knownBlacklistedPeers = Collections.unmodifiableSet(new HashSet<>(knownBlacklistedPeersList));
        }

        maxNumberOfConnectedPublicPeers = Nxt.getIntProperty("nxt.maxNumberOfConnectedPublicPeers");
        connectTimeout = Nxt.getIntProperty("nxt.connectTimeout");
        readTimeout = Nxt.getIntProperty("nxt.readTimeout");
        enableHallmarkProtection = Nxt.getBooleanProperty("nxt.enableHallmarkProtection");
        pushThreshold = Nxt.getIntProperty("nxt.pushThreshold");
        pullThreshold = Nxt.getIntProperty("nxt.pullThreshold");
        dumpPeersVersion = Nxt.getStringProperty("nxt.dumpPeersVersion");
        useWebSockets = Nxt.getBooleanProperty("nxt.useWebSockets");
        webSocketIdleTimeout = Nxt.getIntProperty("nxt.webSocketIdleTimeout");
        blacklistingPeriod = Nxt.getIntProperty("nxt.blacklistingPeriod");
        communicationLoggingMask = Nxt.getIntProperty("nxt.communicationLoggingMask");
        sendToPeersLimit = Nxt.getIntProperty("nxt.sendToPeersLimit");
        usePeersDb = Nxt.getBooleanProperty("nxt.usePeersDb") && ! Constants.isOffline;
        savePeers = usePeersDb && Nxt.getBooleanProperty("nxt.savePeers");
        getMorePeers = Nxt.getBooleanProperty("nxt.getMorePeers");
        cjdnsOnly = Nxt.getBooleanProperty("nxt.cjdnsOnly");
        ignorePeerAnnouncedAddress = Nxt.getBooleanProperty("nxt.ignorePeerAnnouncedAddress");
        if (useWebSockets && useProxy) {
            logger.debug("Using a proxy, will not create outbound websockets.");
        }

        final List<Future<String>> unresolvedPeers = Collections.synchronizedList(new ArrayList<Future<String>>());

        ThreadPool.runBeforeStart(new Runnable() {

            private void loadPeers(Collection<String> addresses) {
                for (final String address : addresses) {
                    Future<String> unresolvedAddress = sendToPeersService.submit(new Callable<String>() {
                        @Override
                        public String call() {
                            Peer peer = Peers.addPeer(address);
                            return peer == null ? address : null;
                        }
                    });
                    unresolvedPeers.add(unresolvedAddress);
                }
            }

            @Override
            public void run() {
                if (! wellKnownPeers.isEmpty()) {
                    loadPeers(wellKnownPeers);
                }
                if (usePeersDb) {
                    logger.debug("Loading known peers from the database...");
                    loadPeers(PeerDb.loadPeers());
                }
            }
        }, false);

        ThreadPool.runAfterStart(new Runnable() {
            @Override
            public void run() {
                for (Future<String> unresolvedPeer : unresolvedPeers) {
                    try {
                        String badAddress = unresolvedPeer.get(5, TimeUnit.SECONDS);
                        if (badAddress != null) {
                            logger.debug("Failed to resolve peer address: " + badAddress);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException e) {
                        logger.debug("Failed to add peer", e);
                    } catch (TimeoutException e) {
                    }
                }
                logger.debug("Known peers: " + peers.size());
            }
        });

    }

    private static class Init {

        private final static Server peerServer;

        static {
            if (Peers.shareMyAddress) {
                peerServer = new Server();
                ServerConnector connector = new ServerConnector(peerServer);
                final int port = Constants.isTestnet ? TESTNET_PEER_PORT : Peers.myPeerServerPort;
                connector.setPort(port);
                final String host = Nxt.getStringProperty("nxt.peerServerHost");
                connector.setHost(host);
                connector.setIdleTimeout(Nxt.getIntProperty("nxt.peerServerIdleTimeout"));
                connector.setReuseAddress(true);
                peerServer.addConnector(connector);

                ServletHolder peerServletHolder = new ServletHolder(new PeerServlet());
                boolean isGzipEnabled = Nxt.getBooleanProperty("nxt.enablePeerServerGZIPFilter");
                peerServletHolder.setInitParameter("isGzipEnabled", Boolean.toString(isGzipEnabled));
                ServletHandler peerHandler = new ServletHandler();
                peerHandler.addServletWithMapping(peerServletHolder, "/*");
                if (Nxt.getBooleanProperty("nxt.enablePeerServerDoSFilter")) {
                    FilterHolder dosFilterHolder = peerHandler.addFilterWithMapping(DoSFilter.class, "/*", FilterMapping.DEFAULT);
                    dosFilterHolder.setInitParameter("maxRequestsPerSec", Nxt.getStringProperty("nxt.peerServerDoSFilter.maxRequestsPerSec"));
                    dosFilterHolder.setInitParameter("delayMs", Nxt.getStringProperty("nxt.peerServerDoSFilter.delayMs"));
                    dosFilterHolder.setInitParameter("maxRequestMs", Nxt.getStringProperty("nxt.peerServerDoSFilter.maxRequestMs"));
                    dosFilterHolder.setInitParameter("trackSessions", "false");
                    dosFilterHolder.setAsyncSupported(true);
                }
                if (isGzipEnabled) {
                    FilterHolder gzipFilterHolder = peerHandler.addFilterWithMapping(GzipFilter.class, "/*", FilterMapping.DEFAULT);
                    gzipFilterHolder.setInitParameter("methods", "GET,POST");
                    gzipFilterHolder.setAsyncSupported(true);
                }

                peerServer.setHandler(peerHandler);
                peerServer.setStopAtShutdown(true);
                ThreadPool.runBeforeStart(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            peerServer.start();
                            logger.info("Started peer networking server at " + host + ":" + port);
                        } catch (Exception e) {
                            logger.error("Failed to start peer networking server", e);
                            throw new RuntimeException(e.toString(), e);
                        }
                    }
                }, true);
            } else {
                peerServer = null;
                logger.info("shareMyAddress is disabled, will not start peer networking server");
            }
        }

        private static void init() {}

        private Init() {}

    }

    private static final Runnable peerUnBlacklistingThread = new Runnable() {

        @Override
        public void run() {

            try {
                try {

                    long curTime = System.currentTimeMillis();
                    for (PeerImpl peer : peers.values()) {
                        peer.updateBlacklistedStatus(curTime);
                    }

                } catch (Exception e) {
                    logger.debug("Error un-blacklisting peer", e);
                }
            } catch (Throwable t) {
                logger.info("CRITICAL ERROR. PLEASE REPORT TO THE DEVELOPERS.\n" + t.toString());
                t.printStackTrace();
                System.exit(1);
            }

        }

    };

    private static final Runnable peerConnectingThread = new Runnable() {

        @Override
        public void run() {

            try {
                try {

                    if (getNumberOfConnectedPublicPeers() < Peers.maxNumberOfConnectedPublicPeers) {
                        PeerImpl peer = (PeerImpl)getAnyPeer(ThreadLocalRandom.current().nextInt(2) == 0 ? Peer.State.NON_CONNECTED : Peer.State.DISCONNECTED, false);
                        if (peer != null) {
                            peer.connect();
                        }
                    }

                    int now = Nxt.getEpochTime();
                    for (PeerImpl peer : peers.values()) {
                        if (peer.getState() == Peer.State.CONNECTED && now - peer.getLastUpdated() > 3600) {
                            peer.connect();
                        }
                    }

                } catch (Exception e) {
                    logger.debug("Error connecting to peer", e);
                }
            } catch (Throwable t) {
                logger.info("CRITICAL ERROR. PLEASE REPORT TO THE DEVELOPERS.\n" + t.toString());
                t.printStackTrace();
                System.exit(1);
            }

        }

    };

    private static final Runnable getMorePeersThread = new Runnable() {

        private final JSONStreamAware getPeersRequest;
        {
            JSONObject request = new JSONObject();
            request.put("requestType", "getPeers");
            getPeersRequest = JSON.prepareRequest(request);
        }

        private volatile boolean addedNewPeer;
        {
            Peers.addListener(new Listener<Peer>() {
                @Override
                public void notify(Peer peer) {
                    addedNewPeer = true;
                }
            }, Event.NEW_PEER);
        }

        @Override
        public void run() {

            try {
                try {

                    Peer peer = getAnyPeer(Peer.State.CONNECTED, true);
                    if (peer == null) {
                        return;
                    }
                    JSONObject response = peer.send(getPeersRequest);
                    if (response == null) {
                        return;
                    }
                    JSONArray peers = (JSONArray)response.get("peers");
                    Set<String> addedAddresses = new HashSet<>();
                    if (peers != null) {
                        for (Object announcedAddress : peers) {
                            if (addPeer((String) announcedAddress) != null) {
                                addedAddresses.add((String) announcedAddress);
                            }
                        }
                        if (savePeers && addedNewPeer) {
                            updateSavedPeers();
                            addedNewPeer = false;
                        }
                    }

                    JSONArray myPeers = new JSONArray();
                    for (Peer myPeer : Peers.getAllPeers()) {
                        if (! myPeer.isBlacklisted() && myPeer.getAnnouncedAddress() != null
                                && myPeer.getState() == Peer.State.CONNECTED && myPeer.shareAddress()
                                && ! addedAddresses.contains(myPeer.getAnnouncedAddress())
                                && ! myPeer.getAnnouncedAddress().equals(peer.getAnnouncedAddress())) {
                            myPeers.add(myPeer.getAnnouncedAddress());
                        }
                    }
                    if (myPeers.size() > 0) {
                        JSONObject request = new JSONObject();
                        request.put("requestType", "addPeers");
                        request.put("peers", myPeers);
                        peer.send(JSON.prepareRequest(request));
                    }

                } catch (Exception e) {
                    logger.debug("Error requesting peers from a peer", e);
                }
            } catch (Throwable t) {
                logger.info("CRITICAL ERROR. PLEASE REPORT TO THE DEVELOPERS.\n" + t.toString());
                t.printStackTrace();
                System.exit(1);
            }

        }

        private void updateSavedPeers() {
            Set<String> oldPeers = new HashSet<>(PeerDb.loadPeers());
            Set<String> currentPeers = new HashSet<>();
            for (Peer peer : Peers.peers.values()) {
                if (peer.getAnnouncedAddress() != null && ! peer.isBlacklisted()) {
                    currentPeers.add(peer.getAnnouncedAddress());
                }
            }
            Set<String> toDelete = new HashSet<>(oldPeers);
            toDelete.removeAll(currentPeers);
            try {
                Db.beginTransaction();
                PeerDb.deletePeers(toDelete);
	            //logger.debug("Deleted " + toDelete.size() + " peers from the peers database");
                currentPeers.removeAll(oldPeers);
                PeerDb.addPeers(currentPeers);
	            //logger.debug("Added " + currentPeers.size() + " peers to the peers database");
                Db.commitTransaction();
            } catch (Exception e) {
                Db.rollbackTransaction();
                throw e;
            } finally {
                Db.endTransaction();
            }
        }

    };

    static {
        Account.addListener(new Listener<Account>() {
            @Override
            public void notify(Account account) {
                for (PeerImpl peer : Peers.peers.values()) {
                    if (peer.getHallmark() != null && peer.getHallmark().getAccountId() == account.getId()) {
                        Peers.listeners.notify(peer, Peers.Event.WEIGHT);
                    }
                }
            }
        }, Account.Event.BALANCE);
    }

    static {
        if (! Constants.isOffline) {
            ThreadPool.scheduleThread("PeerConnecting", Peers.peerConnectingThread, 5);
            ThreadPool.scheduleThread("PeerUnBlacklisting", Peers.peerUnBlacklistingThread, 1);
            if (Peers.getMorePeers) {
                ThreadPool.scheduleThread("GetMorePeers", Peers.getMorePeersThread, 5);
            }
        }
    }

    public static void init() {
        Init.init();
    }

    public static void shutdown() {
        if (Init.peerServer != null) {
            try {
                Init.peerServer.stop();
            } catch (Exception e) {
                logger.info("Failed to stop peer server", e);
            }
        }
        if (dumpPeersVersion != null) {
            StringBuilder buf = new StringBuilder();
            for (Map.Entry<String,String> entry : announcedAddresses.entrySet()) {
                Peer peer = peers.get(entry.getValue());
                if (peer != null && peer.getState() == Peer.State.CONNECTED && peer.shareAddress() && !peer.isBlacklisted()
                        && peer.getVersion() != null && peer.getVersion().startsWith(dumpPeersVersion)) {
                    buf.append("('").append(entry.getKey()).append("'), ");
                }
            }
            logger.info(buf.toString());
        }
        ThreadPool.shutdownExecutor(sendToPeersService);

    }

    public static boolean addListener(Listener<Peer> listener, Event eventType) {
        return Peers.listeners.addListener(listener, eventType);
    }

    public static boolean removeListener(Listener<Peer> listener, Event eventType) {
        return Peers.listeners.removeListener(listener, eventType);
    }

    static void notifyListeners(Peer peer, Event eventType) {
        Peers.listeners.notify(peer, eventType);
    }

    public static Collection<? extends Peer> getAllPeers() {
        return allPeers;
    }

    public static Collection<? extends Peer> getActivePeers() {
        List<PeerImpl> activePeers = new ArrayList<>();
        for (PeerImpl peer : peers.values()) {
            if (peer.getState() != Peer.State.NON_CONNECTED) {
                activePeers.add(peer);
            }
        }
        return activePeers;
    }
    
    public static Collection<? extends Peer> getPeers(Peer.State state) {
        List<PeerImpl> peerList = new ArrayList<>();
        for (PeerImpl peer : peers.values()) {
            if (peer.getState() == state) {
                peerList.add(peer);
            }
        }
        return peerList;
    }

    public static Peer getPeer(String peerAddress) {
        return peers.get(peerAddress);
    }


    public static Peer addPeer(String announcedAddress) {
        if (announcedAddress == null) {
            return null;
        }
        announcedAddress = announcedAddress.trim();
        Peer peer;
        if ((peer = peers.get(announcedAddress)) != null) {
            return peer;
        }
        String address;
        if ((address = announcedAddresses.get(announcedAddress)) != null && (peer = peers.get(address)) != null) {
            return peer;
        }
        try {
            URI uri = new URI("http://" + announcedAddress);
            String host = uri.getHost();
            if ((peer = peers.get(host)) != null) {
                return peer;
            }
            InetAddress inetAddress = InetAddress.getByName(host);
            return addPeer(inetAddress.getHostAddress(), announcedAddress);
        } catch (URISyntaxException | UnknownHostException e) {
            //logger.debug("Invalid peer address: " + announcedAddress + ", " + e.toString());
            return null;
        }
    }

    public static PeerImpl findOrCreatePeer(String announcedAddress, boolean create) {
        if (announcedAddress == null) {
            return null;
        }
        announcedAddress = announcedAddress.trim();
        PeerImpl peer;
        if ((peer = peers.get(announcedAddress)) != null) {
            return peer;
        }
        String address;
        if ((address = announcedAddresses.get(announcedAddress)) != null && (peer = peers.get(address)) != null) {
            return peer;
        }
        try {
            URI uri = new URI("http://" + announcedAddress);
            String host = uri.getHost();
            if (host == null) {
                return null;
            }
            int port = uri.getPort();
            if ((peer = peers.get(addressWithPort(host, port))) != null) {
                return peer;
            }
            InetAddress inetAddress = InetAddress.getByName(host);
            return findOrCreatePeer(inetAddress.getHostAddress(), port, announcedAddress, create);
        } catch (URISyntaxException | UnknownHostException e) {
            //Logger.logDebugMessage("Invalid peer address: " + announcedAddress + ", " + e.toString());
            return null;
        }
    }


      static PeerImpl findOrCreatePeer(String host) {
         try {
             InetAddress inetAddress = InetAddress.getByName(host);
             return findOrCreatePeer(inetAddress, null, true);
         } catch (UnknownHostException e) {
             return null;
         }
     }

      static PeerImpl findOrCreatePeer(final InetAddress inetAddress, final String announcedAddress, final boolean create) {

         if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress() || inetAddress.isLinkLocalAddress()) {
             return null;
         }

         String host = inetAddress.getHostAddress();
         if (Peers.cjdnsOnly && !host.substring(0,2).equals("fc")) {
             return null;
         }
         //re-add the [] to ipv6 addresses lost in getHostAddress() above
         if (host.split(":").length > 2) {
             host = "[" + host + "]";
         }

         PeerImpl peer;
         if ((peer = peers.get(host)) != null) {
             return peer;
         }
         if (!create) {
             return null;
         }

         if (Peers.myAddress != null && Peers.myAddress.equalsIgnoreCase(announcedAddress)) {
             return null;
         }
         if (announcedAddress != null && announcedAddress.length() > MAX_ANNOUNCED_ADDRESS_LENGTH) {
             return null;
         }

         peer = new PeerImpl(host, announcedAddress);
         if (Constants.isTestnet && peer.getPort() != TESTNET_PEER_PORT) {
             logger.debug("Peer " + host + " on testnet is not using port " + TESTNET_PEER_PORT + ", ignoring");
             return null;
         }
         if (!Constants.isTestnet && peer.getPort() == TESTNET_PEER_PORT) {
             logger.debug("Peer " + host + " is using testnet port " + peer.getPort() + ", ignoring");
             return null;
         }
         return peer;
     }

    static PeerImpl findOrCreatePeer(final String address, int port, final String announcedAddress, final boolean create) {

        if (Peers.cjdnsOnly && !address.substring(0,2).equals("fc")) {
            return null;
        }

        //re-add the [] to ipv6 addresses lost in getHostAddress() above
        String cleanAddress = address;
        if (cleanAddress.split(":").length > 2) {
            cleanAddress = "[" + cleanAddress + "]";
        }

        cleanAddress = addressWithPort(cleanAddress, port);

        PeerImpl peer;
        if ((peer = peers.get(cleanAddress)) != null) {
            return peer;
        }
        String peerAddress = normalizeHostAndPort(cleanAddress);
        if (peerAddress == null) {
            return null;
        }
        if ((peer = peers.get(peerAddress)) != null) {
            return peer;
        }

        if (!create) {
            return null;
        }

        String announcedPeerAddress = address.equals(announcedAddress) ? peerAddress : normalizeHostAndPort(announcedAddress);

        if (Peers.myAddress != null && Peers.myAddress.length() > 0 && Peers.myAddress.equalsIgnoreCase(announcedPeerAddress)) {
            return null;
        }

        peer = new PeerImpl(peerAddress, announcedPeerAddress);
        if (Constants.isTestnet && peer.getPort() > 0 && peer.getPort() != TESTNET_PEER_PORT) {
            logger.debug("Peer " + peerAddress + " on testnet is not using port " + TESTNET_PEER_PORT + ", ignoring");
            return null;
        }
        if (!Constants.isTestnet && peer.getPort() > 0 && peer.getPort() == TESTNET_PEER_PORT) {
            logger.debug("Peer " + peerAddress + " is using testnet port " + peer.getPort() + ", ignoring");
            return null;
        }
        return peer;
    }

    static String addressWithPort(String address) {
        try {
            URI uri = new URI("http://" + address.trim());
            return addressWithPort(uri.getHost(), uri.getPort());
        } catch (URISyntaxException e) {
            return null;
        }
    }

    static String addressWithPort(String host, int port) {
        return port > 0 && port != Peers.getDefaultPeerPort() ? host + ":" + port : host;
    }

    static void setAnnouncedAddress(PeerImpl peer, String newAnnouncedAddress) {
        Peer oldPeer = peers.get(peer.getPeerAddress());
        if (oldPeer != null) {
            String oldAnnouncedAddress = oldPeer.getAnnouncedAddress();
            if (oldAnnouncedAddress != null && !oldAnnouncedAddress.equals(newAnnouncedAddress)) {
                logger.debug("Removing old announced address " + oldAnnouncedAddress + " for peer " + oldPeer.getPeerAddress());
                selfAnnouncedAddresses.remove(oldAnnouncedAddress);
            }
        }
        if (newAnnouncedAddress != null) {
            String oldHost = selfAnnouncedAddresses.put(newAnnouncedAddress, peer.getPeerAddress());
            if (oldHost != null && !peer.getPeerAddress().equals(oldHost)) {
                logger.debug("Announced address " + newAnnouncedAddress + " now maps to peer " + peer.getPeerAddress()
                        + ", removing old peer " + oldHost);
                oldPeer = peers.remove(oldHost);
                if (oldPeer != null) {
                    Peers.notifyListeners(oldPeer, Event.REMOVE);
                }
            }
        }
        peer.setAnnouncedAddress(newAnnouncedAddress);
    }

    public static int getDefaultPeerPort() {
        return Constants.isTestnet ? TESTNET_PEER_PORT : DEFAULT_PEER_PORT;
    }

    static PeerImpl addPeer(final String address, final String announcedAddress) {

        //re-add the [] to ipv6 addresses lost in getHostAddress() above
        String clean_address = address;
        if (clean_address.split(":").length > 2) {
            clean_address = "[" + clean_address + "]";
        }
        PeerImpl peer;
        if ((peer = peers.get(clean_address)) != null) {
            return peer;
        }
        String peerAddress = normalizeHostAndPort(clean_address);
        if (peerAddress == null) {
            return null;
        }
        if ((peer = peers.get(peerAddress)) != null) {
            return peer;
        }

        String announcedPeerAddress = address.equals(announcedAddress) ? peerAddress : normalizeHostAndPort(announcedAddress);

        if (Peers.myAddress != null && Peers.myAddress.length() > 0 && Peers.myAddress.equalsIgnoreCase(announcedPeerAddress)) {
            return null;
        }

        peer = new PeerImpl(peerAddress, announcedPeerAddress);
        if (Constants.isTestnet && peer.getPort() > 0 && peer.getPort() != TESTNET_PEER_PORT) {
            logger.debug("Peer " + peerAddress + " on testnet is not using port " + TESTNET_PEER_PORT + ", ignoring");
            return null;
        }
        peers.put(peerAddress, peer);
        if (announcedAddress != null) {
            updateAddress(peer);
        }
        listeners.notify(peer, Event.NEW_PEER);
        return peer;
    }

    static PeerImpl removePeer(PeerImpl peer) {
        if (peer.getAnnouncedAddress() != null) {
            announcedAddresses.remove(peer.getAnnouncedAddress());
        }
        return peers.remove(peer.getPeerAddress());
    }

    static void updateAddress(PeerImpl peer) {
        String oldAddress = announcedAddresses.put(peer.getAnnouncedAddress(), peer.getPeerAddress());
        if (oldAddress != null && !peer.getPeerAddress().equals(oldAddress)) {
            //logger.debug("Peer " + peer.getAnnouncedAddress() + " has changed address from " + oldAddress
            //        + " to " + peer.getPeerAddress());
            Peer oldPeer = peers.remove(oldAddress);
            if (oldPeer != null) {
                Peers.notifyListeners(oldPeer, Peers.Event.REMOVE);
            }
        }
    }

    public static void sendToSomePeers(Block block) {
        JSONObject request = block.getJSONObject();
        request.put("requestType", "processBlock");
        sendToSomePeers(request);
    }

    public static void sendToSomePeers(List<Transaction> transactions) {
        JSONObject request = new JSONObject();
        JSONArray transactionsData = new JSONArray();
        for (Transaction transaction : transactions) {
            transactionsData.add(transaction.getJSONObject());
        }
        request.put("requestType", "processTransactions");
        request.put("transactions", transactionsData);
        sendToSomePeers(request);
    }

      private static void sendToSomePeers(final JSONObject request) {

        sendingService.submit(new Runnable() {
            @Override
            public void run() {
                final JSONStreamAware jsonRequest = JSON.prepareRequest(request);

                int successful = 0;
                List<Future<JSONObject>> expectedResponses = new ArrayList<>();
                for (final Peer peer : peers.values()) {

                    if (Peers.enableHallmarkProtection && peer.getWeight() < Peers.pushThreshold) {
                        continue;
                    }

                    if (!peer.isBlacklisted() && peer.getState() == Peer.State.CONNECTED && peer.getAnnouncedAddress() != null) {
                        Future<JSONObject> futureResponse = sendToPeersService.submit(new Callable<JSONObject>() {
                            @Override
                            public JSONObject call() {
                                return peer.send(jsonRequest);
                            }
                        });
                        expectedResponses.add(futureResponse);
                    }
                    if (expectedResponses.size() >= Peers.sendToPeersLimit - successful) {
                        for (Future<JSONObject> future : expectedResponses) {
                            try {
                                JSONObject response = future.get();
                                if (response != null && response.get("error") == null) {
                                    successful += 1;
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } catch (ExecutionException e) {
                                logger.debug("Error in sendToSomePeers", e);
                            }

                        }
                        expectedResponses.clear();
                    }
                    if (successful >= Peers.sendToPeersLimit) {
                        return;
                    }
                }
            }
        });
    }

    public static void rebroadcastTransactions(List<Transaction> transactions) {
        String info = "Rebroadcasting transactions: ";
        for(Transaction tx : transactions) {
            info = info + Convert.toUnsignedLong(tx.getId()) + " ";
        }
        info = info + "\n to peers ";
        for(Peer peer : peers.values()) {
            if(peer.isRebroadcastTarget()) {
                info = info + peer.getPeerAddress() + " ";
            }
        }
        logger.debug(info);

        JSONObject request = new JSONObject();
        JSONArray transactionsData = new JSONArray();
        for (Transaction transaction : transactions) {
            transactionsData.add(transaction.getJSONObject());
        }
        request.put("requestType", "processTransactions");
        request.put("transactions", transactionsData);

        final JSONObject requestFinal = request;

        sendingService.submit(new Runnable() {
            @Override
            public void run() {
                final JSONStreamAware jsonRequest = JSON.prepareRequest(requestFinal);

                for (final Peer peer : peers.values()) {
                    if(peer.isRebroadcastTarget()) {
                        sendToPeersService.submit(new Callable<JSONObject>() {
                            @Override
                            public JSONObject call() {
                                return peer.send(jsonRequest);
                            }
                        });
                    }
                }
            }
        });

        sendToSomePeers(request); // send to some normal peers too
    }

    public static Peer getBlockFeederPeer() {
        Peer peer = null;
        int curTime = Nxt.getEpochTime();

        Set<Map.Entry<String, Integer>> entries = priorityBlockFeeders.entrySet();
        for(Map.Entry<String, Integer> e : entries) {
            if(curTime - e.getValue() > priorityFeederInterval) {
                e.setValue(curTime);
                peer = addPeer(e.getKey());
                break;
            }
        }

        return peer != null ? peer : getAnyPeer(Peer.State.CONNECTED, true);
    }

    public static boolean addPeer(Peer peer) {
        if (addPeer((PeerImpl) peer)) {
            listeners.notify(peer, Event.NEW_PEER);
            return true;
        }
        return false;
    }

    public static Peer getAnyPeer(Peer.State state, boolean applyPullThreshold) {

        if(connectWellKnownFinished == false) {
            int wellKnownConnected = 0;
            for(Peer peer : peers.values()) {
                if(peer.isWellKnown() && peer.getState() == Peer.State.CONNECTED) {
                    wellKnownConnected++;
                }
            }
            if(wellKnownConnected >= connectWellKnownFirst) {
                connectWellKnownFinished = true;
                logger.info("Finished connecting to " + connectWellKnownFirst + " well known peers.");
                logger.info("You can open your Burst Wallet in your favorite browser with: http://127.0.0.1:8125 or http://localhost:8125");
            }
        }

        List<Peer> selectedPeers = new ArrayList<>();
        for (Peer peer : peers.values()) {
            if (! peer.isBlacklisted() && peer.getState() == state && peer.shareAddress()
                    && (!applyPullThreshold || ! Peers.enableHallmarkProtection || peer.getWeight() >= Peers.pullThreshold)
                    && (connectWellKnownFinished || peer.getState() == Peer.State.CONNECTED || peer.isWellKnown())) {
                selectedPeers.add(peer);
            }
        }

        if (selectedPeers.size() > 0) {
            if (! Peers.enableHallmarkProtection) {
                return selectedPeers.get(ThreadLocalRandom.current().nextInt(selectedPeers.size()));
            }

            long totalWeight = 0;
            for (Peer peer : selectedPeers) {
                long weight = peer.getWeight();
                if (weight == 0) {
                    weight = 1;
                }
                totalWeight += weight;
            }

            long hit = ThreadLocalRandom.current().nextLong(totalWeight);
            for (Peer peer : selectedPeers) {
                long weight = peer.getWeight();
                if (weight == 0) {
                    weight = 1;
                }
                if ((hit -= weight) < 0) {
                    return peer;
                }
            }
        }
        return null;
    }

    static String normalizeHostAndPort(String address) {
        try {
            if (address == null) {
                return null;
            }
            URI uri = new URI("http://" + address.trim());
            String host = uri.getHost();
            if (host == null || host.equals("") || host.equals("localhost") ||
                                host.equals("127.0.0.1") || host.equals("[0:0:0:0:0:0:0:1]")) {
                return null;
            }
            InetAddress inetAddress = InetAddress.getByName(host);
            if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress() ||
                                                   inetAddress.isLinkLocalAddress()) {
                return null;
            }
            int port = uri.getPort();
            return port == -1 ? host : host + ':' + port;
        } catch (URISyntaxException |UnknownHostException e) {
            return null;
        }
    }

    private static int getNumberOfConnectedPublicPeers() {
        int numberOfConnectedPeers = 0;
        for (Peer peer : peers.values()) {
            if (peer.getState() == Peer.State.CONNECTED && peer.getAnnouncedAddress() != null
                   && (! Peers.enableHallmarkProtection || peer.getWeight() > 0)) {
                numberOfConnectedPeers++;
            }
        }
        return numberOfConnectedPeers;
    }

    private Peers() {} // never

}
