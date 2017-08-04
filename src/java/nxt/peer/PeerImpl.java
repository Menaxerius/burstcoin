package nxt.peer;

import nxt.Account;
import nxt.BlockchainProcessor;
import nxt.Constants;
import nxt.Nxt;
import nxt.NxtException;
import nxt.util.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

final class PeerImpl implements Peer {

    private static final boolean useProxy = System.getProperty("socksProxyHost") != null ||
                                            System.getProperty("http.proxyHost") != null;

    private final String peerAddress;
    private final PeerWebSocket webSocket;
    private volatile PeerWebSocket inboundSocket;
    private boolean useWebSocket;
    private volatile String announcedAddress;
    private volatile int port;
    private volatile boolean shareAddress;
    private volatile Hallmark hallmark;
    private volatile String platform;
    private volatile String application;
    private volatile String version;
    private volatile boolean isOldVersion;
    private volatile long adjustedWeight;
    private volatile long blacklistingTime;
    private volatile String blacklistingCause;
    private volatile State state;
    private volatile long downloadedVolume;
    private volatile long uploadedVolume;
    private volatile int lastUpdated;
    private volatile int lastConnectAttempt;
    private volatile int lastInboundRequest;
    private volatile long hallmarkBalance = -1;
    private volatile int hallmarkBalanceHeight;


    PeerImpl(String peerAddress, String announcedAddress) {
        this.peerAddress = peerAddress;
        this.announcedAddress = announcedAddress;
        try {
            this.port = new URI("http://" + announcedAddress).getPort();
        } catch (URISyntaxException ignore) {
        }
        this.state = State.NON_CONNECTED;
        this.shareAddress = true;
        this.webSocket = new PeerWebSocket();
        this.useWebSocket = Peers.useWebSockets && !Peers.useProxy;
    }

    @Override
    public String getPeerAddress() {
        return peerAddress;
    }

    @Override
    public State getState() {
        return state;
    }

    void setState(State state) {
        if (state != State.CONNECTED)
            webSocket.close();
        if (this.state == state) {
            return;
        }
        if (this.state == State.NON_CONNECTED) {
            this.state = state;
            Peers.notifyListeners(this, Peers.Event.ADDED_ACTIVE_PEER);
        } else if (state != State.NON_CONNECTED) {
            this.state = state;
            Peers.notifyListeners(this, Peers.Event.CHANGED_ACTIVE_PEER);
        } else {
            this.state = state;
        }
    }

    @Override
    public long getDownloadedVolume() {
        return downloadedVolume;
    }

    void updateDownloadedVolume(long volume) {
        synchronized (this) {
            downloadedVolume += volume;
        }
        Peers.notifyListeners(this, Peers.Event.DOWNLOADED_VOLUME);
    }

    @Override
    public long getUploadedVolume() {
        return uploadedVolume;
    }

    void updateUploadedVolume(long volume) {
        synchronized (this) {
            uploadedVolume += volume;
        }
        Peers.notifyListeners(this, Peers.Event.UPLOADED_VOLUME);
    }

    @Override
    public String getVersion() {
        return version;
    }

    void setVersion(String version) {
        this.version = version;
        if (version != null && version.length() > Peers.MAX_VERSION_LENGTH) {
            throw new IllegalArgumentException("Invalid version length: " + version.length());
        }
        if (this.version != null && this.version.equals(version)) {
            return;
        }
        isOldVersion = false;
        if (Nxt.APPLICATION.equals(application) && version != null) {
            String[] versions = version.split("\\.");
            if (versions.length < Constants.MIN_VERSION.length) {
                isOldVersion = true;
            } else {
                for (int i = 0; i < Constants.MIN_VERSION.length; i++) {
                    try {
                        int v = Integer.parseInt(versions[i]);
                        if (v > Constants.MIN_VERSION[i]) {
                            isOldVersion = false;
                            break;
                        } else if (v < Constants.MIN_VERSION[i]) {
                            isOldVersion = true;
                            break;
                        }
                    } catch (NumberFormatException e) {
                        isOldVersion = true;
                        break;
                    }
                }
            }
            if (isOldVersion) {
                Logger.logDebugMessage(String.format("Blacklisting %s version %s", port, version));
                blacklistingCause = "Old version: " + version;
                lastInboundRequest = 0;
                setState(State.NON_CONNECTED);
                Peers.notifyListeners(this, Peers.Event.BLACKLIST);
            }
        }
    }

    @Override
    public String getApplication() {
        return application;
    }

    void setApplication(String application) {
        if (application == null || application.length() > Peers.MAX_APPLICATION_LENGTH) {
            throw new IllegalArgumentException("Invalid application");
        }
        this.application = application;
    }

    @Override
    public String getPlatform() {
        return platform;
    }

    void setPlatform(String platform) {
        if (platform != null && platform.length() > Peers.MAX_PLATFORM_LENGTH) {
            throw new IllegalArgumentException("Invalid platform length: " + platform.length());
        }
        this.platform = platform;
    }

    @Override
    public String getSoftware() {
        return Convert.truncate(application, "?", 10, false)
                + " (" + Convert.truncate(version, "?", 10, false) + ")"
                + " @ " + Convert.truncate(platform, "?", 10, false);
    }

    @Override
    public boolean shareAddress() {
        return shareAddress;
    }


    void setShareAddress(boolean shareAddress) {
        this.shareAddress = shareAddress;
    }

    @Override
    public String getAnnouncedAddress() {
        return announcedAddress;
    }

    void setAnnouncedAddress(String announcedAddress) {
        if (announcedAddress != null && announcedAddress.length() > Peers.MAX_ANNOUNCED_ADDRESS_LENGTH) {
            throw new IllegalArgumentException("Announced address too long: " + announcedAddress.length());
        }
        this.announcedAddress = announcedAddress;
        if (announcedAddress != null) {
            try {
                this.port = new URI("http://" + announcedAddress).getPort();
            } catch (URISyntaxException e) {
                this.port = -1;
            }
        } else {
            this.port = -1;
        }
    }

    public int getPort() {
        return port <= 0 ? Peers.getDefaultPeerPort() : port;
    }

    @Override
    public boolean isWellKnown() {
        return announcedAddress != null && Peers.wellKnownPeers.contains(announcedAddress);
    }

    @Override
    public boolean isRebroadcastTarget() {
        return announcedAddress != null && Peers.rebroadcastPeers.contains(announcedAddress);
    }

    @Override
    public Hallmark getHallmark() {
        return hallmark;
    }

    @Override
    public int getWeight() {
        if (hallmark == null) {
            return 0;
        }
        if (hallmarkBalance == -1 || hallmarkBalanceHeight < Nxt.getBlockchain().getHeight() - 60) {
            long accountId = hallmark.getAccountId();
            Account account = Account.getAccount(accountId);
            hallmarkBalance = account == null ? 0 : account.getBalanceNQT();
            hallmarkBalanceHeight = Nxt.getBlockchain().getHeight();
        }

        return (int) (adjustedWeight * (hallmarkBalance / Constants.ONE_NXT) / Constants.MAX_BALANCE_NXT);
    }

    @Override
    public boolean isBlacklisted() {
        return blacklistingTime > 0 || isOldVersion || Peers.knownBlacklistedPeers.contains(peerAddress)
                || (announcedAddress != null && Peers.knownBlacklistedPeers.contains(announcedAddress));
    }

    @Override
    public void blacklist(Exception cause) {
        if (cause instanceof NxtException.NotCurrentlyValidException || cause instanceof BlockchainProcessor.BlockOutOfOrderException
                || cause instanceof SQLException || cause.getCause() instanceof SQLException) {
            // don't blacklist peers just because a feature is not yet enabled, or because of database timeouts
            // prevents erroneous blacklisting during loading of blockchain from scratch
            return;
        }
        if (cause instanceof ParseException && Errors.END_OF_FILE.equals(cause.toString())) {
            return;
        }
        if (!isBlacklisted()) {
            if (cause instanceof IOException || cause instanceof ParseException || cause instanceof IllegalArgumentException) {
                Logger.logDebugMessage("Blacklisting " + peerAddress + " because of: " + cause.toString());
            } else {
                Logger.logDebugMessage("Blacklisting " + peerAddress + " because of: " + cause.toString(), cause);
            }
        }
        blacklist(cause.toString() == null ? cause.getClass().getName() : cause.toString());
    }

    @Override
    public void blacklist(String cause) {
        blacklistingTime = System.currentTimeMillis();
        blacklistingCause = cause;
        setState(State.NON_CONNECTED);
        lastInboundRequest = 0;
        Peers.notifyListeners(this, Peers.Event.BLACKLIST);
    }

    @Override
    public void blacklist() {
        blacklistingTime = System.currentTimeMillis();
        setState(State.NON_CONNECTED);
        lastInboundRequest = 0;
        Peers.notifyListeners(this, Peers.Event.BLACKLIST);
    }

    @Override
    public void unBlacklist() {
        if (blacklistingTime == 0) {
            return;
        }
        Logger.logDebugMessage("Unblacklisting " + peerAddress);
        setState(State.NON_CONNECTED);
        blacklistingTime = 0;
        blacklistingCause = null;
        Peers.notifyListeners(this, Peers.Event.UNBLACKLIST);
    }

    void updateBlacklistedStatus(long curTime) {
        if (blacklistingTime > 0 && blacklistingTime + Peers.blacklistingPeriod <= curTime) {
            unBlacklist();
        }
    }

    @Override
    public void deactivate() {
        if (state == State.CONNECTED) {
            setState(State.DISCONNECTED);
        } else {
            setState(State.NON_CONNECTED);
        }
        Peers.notifyListeners(this, Peers.Event.DEACTIVATE);
    }

    @Override
    public void remove() {
        webSocket.close();
        Peers.removePeer(this);
        Peers.notifyListeners(this, Peers.Event.REMOVE);
    }

    @Override
    public int getLastUpdated() {
        return lastUpdated;
    }

    void setLastUpdated(int lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Override
    public boolean isInbound() {
        return lastInboundRequest != 0;
    }

    int getLastInboundRequest() {
        return lastInboundRequest;
    }

    void setLastInboundRequest(int now) {
        lastInboundRequest = now;
    }

    void setInboundWebSocket(PeerWebSocket inboundSocket) {
        this.inboundSocket = inboundSocket;
    }

    @Override
    public boolean isInboundWebSocket() {
        PeerWebSocket s;
        return ((s = inboundSocket) != null && s.isOpen());
    }

    @Override
    public boolean isOutboundWebSocket() {
        return webSocket.isOpen();
    }

    @Override
    public String getBlacklistingCause() {
        return blacklistingCause == null ? "unknown" : blacklistingCause;
    }

    int getLastConnectAttempt() {
        return lastConnectAttempt;
    }

    @Override
    public JSONObject send(final JSONStreamAware request, int maxResponseSize) {

        JSONObject response = null;
        String log = null;
        boolean showLog = false;
        HttpURLConnection connection = null;
        String address = announcedAddress != null ? announcedAddress : peerAddress;
        int communicationLoggingMask = Peers.communicationLoggingMask;

        try {
            //
            // Create a new WebSocket session if we don't have one
            //
            if (useWebSocket && !webSocket.isOpen())
                useWebSocket = webSocket.startClient(URI.create("ws://" + peerAddress + ":" + getPort() + "/burst"));
            //
            // Send the request and process the response
            //
            if (useWebSocket) {
                //
                // Send the request using the WebSocket session
                //
                StringWriter wsWriter = new StringWriter(1000);
                request.writeJSONString(wsWriter);
                String wsRequest = wsWriter.toString();
                if (communicationLoggingMask != 0)
                    log = "WebSocket " + peerAddress + ": " + wsRequest;
                String wsResponse = webSocket.doPost(wsRequest);
                updateUploadedVolume(wsRequest.length());
                if (maxResponseSize > 0) {
                    if ((communicationLoggingMask & Peers.LOGGING_MASK_200_RESPONSES) != 0) {
                        log += " >>> " + wsResponse;
                        showLog = true;
                    }
                    if (wsResponse.length() > maxResponseSize)
                        throw new NxtException.NxtIOException("Maximum size exceeded: " + wsResponse.length());
                    try {
                        response = (JSONObject) JSONValue.parseWithException(wsResponse);
                    } catch (org.json.simple.parser.ParseException e) {
                        e.printStackTrace();
                    }
                    updateDownloadedVolume(wsResponse.length());
                }
            } else {
                //
                // Send the request using HTTP
                //
                address = announcedAddress != null ? announcedAddress : peerAddress;
                StringBuilder buf = new StringBuilder("http://");
                buf.append(announcedAddress);
                if (port <= 0) {
                    buf.append(':');
                    buf.append(Constants.isTestnet ? Peers.TESTNET_PEER_PORT : Peers.DEFAULT_PEER_PORT);
                }
                buf.append("/burst");
                URL url = new URL(buf.toString());

                if (Peers.communicationLoggingMask != 0) {
                    StringWriter stringWriter = new StringWriter();
                    request.writeJSONString(stringWriter);
                    log = "\"" + url.toString() + "\": " + stringWriter.toString();
                }

                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setConnectTimeout(Peers.connectTimeout);
                connection.setReadTimeout(Peers.readTimeout);
                connection.setRequestProperty("Accept-Encoding", "gzip");
                connection.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");

                CountingOutputStream cos = new CountingOutputStream(connection.getOutputStream());
                try (Writer writer = new BufferedWriter(new OutputStreamWriter(cos, "UTF-8"))) {
                    request.writeJSONString(writer);
                }
                updateUploadedVolume(cos.getCount());

                if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    CountingInputStream cis = new CountingInputStream(connection.getInputStream());
                    InputStream responseStream = cis;
                    if ("gzip".equals(connection.getHeaderField("Content-Encoding"))) {
                        responseStream = new GZIPInputStream(cis);
                    }
                    if ((Peers.communicationLoggingMask & Peers.LOGGING_MASK_200_RESPONSES) != 0) {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        byte[] buffer = new byte[1024];
                        int numberOfBytes;
                        try (InputStream inputStream = responseStream) {
                            while ((numberOfBytes = inputStream.read(buffer, 0, buffer.length)) > 0) {
                                byteArrayOutputStream.write(buffer, 0, numberOfBytes);
                            }
                        }
                        String responseValue = byteArrayOutputStream.toString("UTF-8");
                        if (responseValue.length() > 0 && responseStream instanceof GZIPInputStream) {
                            log += String.format("[length: %d, compression ratio: %.2f]", cis.getCount(), (double) cis.getCount() / (double) responseValue.length());
                        }
                        log += " >>> " + responseValue;
                        showLog = true;
                        response = (JSONObject) JSONValue.parse(responseValue);
                    } else {
                        try (Reader reader = new BufferedReader(new InputStreamReader(responseStream, "UTF-8"))) {
                            response = (JSONObject) JSONValue.parse(reader);
                        }
                    }
                    updateDownloadedVolume(cis.getCount());
                } else {

                    if ((Peers.communicationLoggingMask & Peers.LOGGING_MASK_NON200_RESPONSES) != 0) {
                        log += " >>> Peer responded with HTTP " + connection.getResponseCode() + " code!";
                        showLog = true;
                    }
                    if (state == State.CONNECTED) {
                        setState(State.DISCONNECTED);
                    } else {
                        setState(State.NON_CONNECTED);
                    }
                    response = null;
                }
            }
        } catch (RuntimeException|IOException e) {
            if (state == State.CONNECTED ||
                    !(e instanceof UnknownHostException || e instanceof SocketTimeoutException ||
                            e instanceof SocketException || Errors.END_OF_FILE.equals(e.getMessage()))) {
                Logger.logDebugMessage(String.format("Error sending JSON request to %s: %s",
                        peerAddress, e.getMessage()!=null ? e.getMessage() : e.toString()));
            }
            if ((Peers.communicationLoggingMask & Peers.LOGGING_MASK_EXCEPTIONS) != 0) {
                log += " >>> " + e.toString();
                showLog = true;
            }
            if (state == State.CONNECTED) {
                setState(State.DISCONNECTED);
            }
            response = null;
        }

        if (showLog) {
            Logger.logMessage(log + "\n");
        }

        if (connection != null) {
            connection.disconnect();
        }

        return response;
    }


    @Override
    public JSONObject send(final JSONStreamAware request) {
        return send(request, Peers.MAX_RESPONSE_SIZE);
    }

    @Override
    public int compareTo(Peer o) {
        if (getWeight() > o.getWeight()) {
            return -1;
        } else if (getWeight() < o.getWeight()) {
            return 1;
        }
        return 0;
    }

    void connect() {
        lastConnectAttempt = Nxt.getEpochTime();
        try {
            if (!Peers.ignorePeerAnnouncedAddress && announcedAddress != null) {
                try {
                    URI uri = new URI("http://" + announcedAddress);
                    InetAddress inetAddress = InetAddress.getByName(uri.getHost());
                    if (!inetAddress.equals(InetAddress.getByName(peerAddress))) {
                        Logger.logDebugMessage("Announced address " + announcedAddress + " now points to " + inetAddress.getHostAddress() + ", replacing peer " + peerAddress);
                        Peers.removePeer(this);
                        PeerImpl newPeer = Peers.findOrCreatePeer(inetAddress, announcedAddress, true);
                        if (newPeer != null) {
                            Peers.addPeer(newPeer);
                            newPeer.connect();
                        }
                        return;
                    }
                } catch (URISyntaxException | UnknownHostException e) {
                    blacklist(e);
                    return;
                }
            }
            JSONObject response = send(Peers.myPeerInfoRequest);
            if (response != null) {
                if (response.get("error") != null) {
                    setState(State.NON_CONNECTED);
                    return;
                }
                setApplication((String)response.get("application"));
                lastUpdated = lastConnectAttempt;
                setVersion((String) response.get("version"));
                setPlatform((String) response.get("platform"));
                shareAddress = Boolean.TRUE.equals(response.get("shareAddress"));
                analyzeHallmark(announcedAddress, (String) response.get("hallmark"));

                if (!Peers.ignorePeerAnnouncedAddress) {
                    String newAnnouncedAddress = Convert.emptyToNull((String) response.get("announcedAddress"));
                    if (newAnnouncedAddress != null) {
                        newAnnouncedAddress = Peers.addressWithPort(newAnnouncedAddress.toLowerCase());
                        if (newAnnouncedAddress != null) {
                            if (!verifyAnnouncedAddress(newAnnouncedAddress)) {
                                Logger.logDebugMessage("Connect: new announced address for " + peerAddress + " not accepted");
                                if (!verifyAnnouncedAddress(announcedAddress)) {
                                    Logger.logDebugMessage("Connect: old announced address for " + peerAddress + " no longer valid");
                                    Peers.setAnnouncedAddress(this, peerAddress);
                                }
                                setState(State.NON_CONNECTED);
                                return;
                            }
                            if (!newAnnouncedAddress.equals(announcedAddress)) {
                                Logger.logDebugMessage("Connect: peer " + peerAddress + " has new announced address " + newAnnouncedAddress + ", old is " + announcedAddress);
                                int oldPort = getPort();
                                Peers.setAnnouncedAddress(this, newAnnouncedAddress);
                                if (getPort() != oldPort) {
                                    // force checking connectivity to new announced port
                                    setState(State.NON_CONNECTED);
                                    return;
                                }
                            }
                        }
                    } else {
                        Peers.setAnnouncedAddress(this, peerAddress);
                    }
                }

                if (announcedAddress == null) {
                    if (hallmark == null) {
                        Peers.setAnnouncedAddress(this, peerAddress);
                        Logger.logDebugMessage("Connected to peer without announced address, setting to " + peerAddress);
                    } else {
                        setState(State.NON_CONNECTED);
                        return;
                    }
                }
                if (!isOldVersion) {
                    setState(State.CONNECTED);
                } else if (!isBlacklisted()) {
                    blacklist("Old version: " + version);
                }
            } else {
                //Logger.logDebugMessage("Failed to connect to peer " + peerAddress);
                setState(State.NON_CONNECTED);
            }
        } catch (RuntimeException e) {
            blacklist(e);
        }
    }


    boolean verifyAnnouncedAddress(String newAnnouncedAddress) {
        if (newAnnouncedAddress == null) {
            return true;
        }
        try {
            URI uri = new URI("http://" + newAnnouncedAddress);
            int announcedPort = uri.getPort() == -1 ? Peers.getDefaultPeerPort() : uri.getPort();
            InetAddress address = InetAddress.getByName(peerAddress);
            for (InetAddress inetAddress : InetAddress.getAllByName(uri.getHost())) {
                if (inetAddress.equals(address)) {
                    return true;
                }
            }
            Logger.logDebugMessage("Announced address " + newAnnouncedAddress + " does not resolve to " + peerAddress);
        } catch (UnknownHostException|URISyntaxException e) {
            Logger.logDebugMessage(e.toString());
            blacklist(e);
        }
        return false;
    }
    boolean analyzeHallmark(String address, final String hallmarkString) {

        if (hallmarkString == null && this.hallmark == null) {
            return true;
        }

        if (this.hallmark != null && this.hallmark.getHallmarkString().equals(hallmarkString)) {
            return true;
        }

        if (hallmarkString == null) {
            this.hallmark = null;
            return true;
        }

        try {
            URI uri = new URI("http://" + address.trim());
            String host = uri.getHost();

            Hallmark hallmark = Hallmark.parseHallmark(hallmarkString);
            if (!hallmark.isValid()
                    || !(hallmark.getHost().equals(host) || InetAddress.getByName(host).equals(InetAddress.getByName(hallmark.getHost())))) {
                //Logger.logDebugMessage("Invalid hallmark for " + host + ", hallmark host is " + hallmark.getHost());
                return false;
            }
            this.hallmark = hallmark;
            long accountId = Account.getId(hallmark.getPublicKey());
            List<PeerImpl> groupedPeers = new ArrayList<>();
            int mostRecentDate = 0;
            long totalWeight = 0;
            for (PeerImpl peer : Peers.allPeers) {
                if (peer.hallmark == null) {
                    continue;
                }
                if (accountId == peer.hallmark.getAccountId()) {
                    groupedPeers.add(peer);
                    if (peer.hallmark.getDate() > mostRecentDate) {
                        mostRecentDate = peer.hallmark.getDate();
                        totalWeight = peer.getHallmarkWeight(mostRecentDate);
                    } else {
                        totalWeight += peer.getHallmarkWeight(mostRecentDate);
                    }
                }
            }

            for (PeerImpl peer : groupedPeers) {
                peer.adjustedWeight = Constants.MAX_BALANCE_NXT * peer.getHallmarkWeight(mostRecentDate) / totalWeight;
                Peers.notifyListeners(peer, Peers.Event.WEIGHT);
            }

            return true;

        } catch (UnknownHostException ignore) {
        } catch (URISyntaxException | RuntimeException e) {
            Logger.logDebugMessage("Failed to analyze hallmark for peer " + address + ", " + e.toString(), e);
        }
        return false;

    }

    private int getHallmarkWeight(int date) {
        if (hallmark == null || ! hallmark.isValid() || hallmark.getDate() != date) {
            return 0;
        }
        return hallmark.getWeight();
    }

}
