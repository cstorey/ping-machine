module Server where

import qualified Lib

import qualified Network.Socket            as S
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import qualified Data.Binary as Binary
import qualified System.Environment as Env
import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.Async as Async
import qualified Data.Map as Map
import Data.List ((\\))
import Control.Applicative ((<|>))
import qualified Data.Time.Clock.POSIX as Clock
import Data.Maybe (listToMaybe)


-- import qualified Data.ByteString as B

import qualified Control.Concurrent as C
import Control.Monad
import qualified Control.Exception as E

type ResponsesOutQ resp =  STM.TQueue (Maybe resp)
type RequestsInQ sender req =  STM.TQueue (sender, Maybe req)
type RequestsOutQ req =  STM.TQueue (Maybe req)


newtype ClientID = ClientID Int
    deriving (Show, Eq, Ord)

-- Identifier for an _incoming_ request
newtype PeerID = PeerID Int
    deriving (Show, Eq, Ord)


-- Identifier for an outgoing request
newtype PeerName = PeerName { unPeerName :: String }
    deriving (Show, Eq, Ord)

data ProcessorMessage = Reply ClientID Lib.ClientResponse
    | PeerRequest PeerName Lib.PeerRequest
    | PeerReply PeerID Lib.PeerResponse
    deriving (Show)

type STMRespChanMap xid resp = STM.TVar (Map.Map xid (ResponsesOutQ resp))

data Tick = Tick Clock.POSIXTime

data ProtocolEnv = ProtocolEnv {
    peerNames :: [PeerName]
}

data ProtocolState = ProtocolState {
    bings :: Int,
    pending :: Map.Map Int ProcessorMessage
} deriving (Show)

type ProtoStateMachine = RWS.RWS ProtocolEnv [ProcessorMessage] ProtocolState ()

main ::  IO ()
main = S.withSocketsDo $ do
    clientPort : peerPort : peerPorts <- Env.getArgs
    clientAddr <- resolve clientPort
    peerListenAddr <- resolve peerPort
    ids <- STM.atomically $ STM.newTVar 0
    clientReqQ <- STM.atomically STM.newTQueue:: IO (STM.TQueue (ClientID,Maybe Lib.ClientRequest))
    peerReqInQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerID,Maybe Lib.PeerRequest))
    peerRespQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerName,Maybe Lib.PeerResponse))
    ticks <- STM.atomically STM.newTQueue :: IO (STM.TQueue ((),Maybe Tick))
    clients <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map ClientID (ResponsesOutQ Lib.ClientResponse)))
    requestToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map PeerName (ResponsesOutQ Lib.PeerRequest)))
    responsesToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map PeerID (ResponsesOutQ Lib.PeerResponse)))
    -- We also need to start a peer manager. This will start a single process
    -- for each known peer, attempt to connect, then relay messages to/from
    -- peers.
    let race = Async.race_
    (runListener (ClientID <$> nextId ids) clientAddr clients clientReqQ) `race`
        (runListener (PeerID <$> nextId ids) peerListenAddr responsesToPeers peerReqInQ) `race`
        (runOutgoing (PeerName <$> peerPorts) requestToPeers peerRespQ) `race`
        (runTicker ticks) `race`
        (runModel clientReqQ peerReqInQ peerRespQ clients ticks requestToPeers responsesToPeers)

nextId :: STM.TVar Int -> IO Int
nextId ids = STM.atomically $ do
    n <- STM.readTVar ids
    STM.writeTVar ids (n+1)
    return n


-- Supervisor
runOutgoing :: [PeerName]
            -> STMRespChanMap PeerName Lib.PeerRequest
            -> RequestsInQ PeerName Lib.PeerResponse
            -> IO ()
runOutgoing seedPeers peers peerRespQ = do
    putStrLn $ "peers:" ++ show seedPeers
    processes <- STM.atomically $ STM.newTVar $ Map.empty

    void $ forever $ do
        runningProcesses <- Map.elems <$> STM.readTVarIO processes
        forM_ (seedPeers \\ runningProcesses) $ \name -> do
            q <- STM.newTQueueIO
            a <- Async.async $ runPeer peerRespQ name q
            STM.atomically $ do
                STM.modifyTVar peers $ Map.insert name q
                STM.modifyTVar processes $ Map.insert a name
            putStrLn $ "Starting: " ++ show name
        do
            procNames <- Map.elems <$> STM.readTVarIO processes
            putStrLn $ "procs:" ++ show procNames

        (peerNamep, ret) <- STM.atomically $ do
            procs <- STM.readTVar processes
            (a, retOrError) <- Async.waitAnyCatchSTM $ Map.keys procs
            let namep = Map.lookup a procs
            STM.writeTVar processes $ Map.delete a procs
            return (namep, retOrError)

        putStrLn $ "runOutgoing : " ++ show (peerNamep, ret)
        C.threadDelay 1000000

connect :: S.AddrInfo -> IO S.Socket
connect addr = do
    sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    S.connect sock $ S.addrAddress addr
    return sock

runPeer :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp) => RequestsInQ PeerName req -> PeerName -> ResponsesOutQ resp -> IO ()
runPeer fromPeerQ name toPeerQ = do
    addrInfo <- resolve $ unPeerName name
    (is, os) <- streamsOf =<< connect addrInfo

    processClientRequests fromPeerQ name toPeerQ (is, os)
    error $ "runPeer: " ++ show name

runListener :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp, Show xid, Ord xid)
            => IO xid
            -> S.AddrInfo
            -> STM.TVar (Map.Map xid (ResponsesOutQ resp))
            -> RequestsInQ xid req
            -> IO ()
runListener newId addr clients reqs =
    E.bracket (listenFor addr) S.close (runAcceptor newId $ handleConn clients reqs)

resolve :: S.ServiceName -> IO S.AddrInfo
resolve port = do
    let hints = S.defaultHints {
            S.addrFlags = [S.AI_PASSIVE]
        , S.addrSocketType = S.Stream
        }
    addr:_ <- S.getAddrInfo (Just hints) Nothing (Just port)
    return addr

listenFor :: S.AddrInfo -> IO S.Socket
listenFor addr = do
    sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    S.setSocketOption sock S.ReuseAddr 1
    S.bind sock (S.addrAddress addr)
    S.listen sock 10
    putStrLn . show =<< S.getSocketName sock
    return sock

streamsOf :: (Binary.Binary a, Binary.Binary b) => S.Socket -> IO (Streams.InputStream a, Streams.OutputStream b)
streamsOf client = do
    (is, os) <- (Streams.socketToStreams client)
    (,) <$> BStreams.decodeInputStream is <*> BStreams.encodeOutputStream os


runAcceptor :: (Binary.Binary req, Binary.Binary resp)
            => IO xid
            -> (xid -> (Streams.InputStream req, Streams.OutputStream resp) -> IO ())
            -> S.Socket
            -> IO ()
runAcceptor newId handler listener = go
    where
    go = void $ forever $ do
            (client, x) <- S.accept listener
            putStrLn $ show (client, x)
            n <- newId
            void $ C.forkIO $ do
                E.bracket (streamsOf client) (const $ S.close client) (handler n)

handleConn :: (Show req, Show resp, Show xid, Ord xid)
            => STM.TVar (Map.Map xid (ResponsesOutQ resp))
            -> RequestsInQ xid req
            -> xid
            -> (Streams.InputStream req, Streams.OutputStream resp)
            -> IO ()

handleConn clients modelQ clientId (is,os) = do
    q <- STM.atomically $ do
        q <- STM.newTQueue
        STM.modifyTVar clients $ Map.insert clientId q
        return q
    processClientRequests modelQ clientId q (is, os)

processClientRequests :: (Show xid, Show req, Show resp) =>
       RequestsInQ xid req
    -> xid
    -> STM.TQueue (Maybe resp)
    -> (Streams.InputStream req, Streams.OutputStream resp)
    -> IO ()

processClientRequests outQ clientId inQ (is, os) = do
    Async.concurrently_ (reader clientId inQ) (writer inQ)
    putStrLn $ "Done: " ++ show clientId
    where
    reader senderId writerQ = do
        it <- Streams.read is
        case it of
            Just msg -> do
                putStrLn $ "<- " ++ show clientId ++ ":" ++ show msg
                STM.atomically $ STM.writeTQueue outQ $ (senderId, Just (msg))
                reader senderId writerQ
            Nothing -> STM.atomically $ STM.writeTQueue writerQ Nothing

    writer sender =  do
        msg <- STM.atomically $ STM.readTQueue sender
        putStrLn $ "-> " ++ show clientId ++ ":" ++ show msg
        Streams.write msg os
        maybe (return ()) (const $ writer sender) msg


runTicker :: STM.TQueue ((), Maybe Tick) -> IO ()
runTicker ticks = void $ forever $ do
    t <- Clock.getPOSIXTime
    STM.atomically $ STM.writeTQueue ticks ((), Just $ Tick t)
    putStrLn $ "Tick: " ++ show t
    C.threadDelay 1000000

--- Model bits

runModel :: RequestsInQ ClientID Lib.ClientRequest
            -> RequestsInQ PeerID Lib.PeerRequest
            -> RequestsInQ PeerName Lib.PeerResponse
            -> STMRespChanMap ClientID Lib.ClientResponse
            -> RequestsInQ () Tick
            -> STMRespChanMap PeerName Lib.PeerRequest
            -> STMRespChanMap PeerID Lib.PeerResponse
            -> IO ()
runModel modelQ peerReqInQ peerRespInQ clients ticks peerOuts responsePeers = do
    stateRef <- STM.atomically $ STM.newTVar $ ProtocolState 0 Map.empty

    let protocolEnv = ProtocolEnv <$> (Map.keys <$> STM.readTVar peerOuts)
    let processClientMessage = processMessageSTM stateRef protocolEnv modelQ processClientRequestMessage
    let processTickMessage = processMessageSTM stateRef protocolEnv ticks processTick
    let processPeerRequest = processMessageSTM stateRef protocolEnv peerReqInQ processPeerRequestMessage
    let processPeerResponse = processMessageSTM stateRef protocolEnv peerRespInQ processPeerResponseMessage

    forever $ STM.atomically $ do
        outputs <- processClientMessage <|> processPeerRequest <|> processTickMessage <|> processPeerResponse
        sendMessages clients peerOuts responsePeers outputs

processMessageSTM :: STM.TVar ProtocolState
                  -> STM.STM ProtocolEnv
                  -> RequestsInQ xid req
                  -> (xid -> req -> ProtoStateMachine)
                  -> STM.STM [ProcessorMessage]
processMessageSTM stateRef envSTM reqQ process = do
    (sender, m) <- STM.readTQueue reqQ
    case m of
        Just msg -> do
            s <- STM.readTVar stateRef
            env <- envSTM
            let ((), s', toSend) = RWS.runRWS (process sender msg) env s
            STM.writeTVar stateRef s'
            return toSend
        Nothing -> return []

sendMessages :: STMRespChanMap ClientID Lib.ClientResponse
             -> STMRespChanMap PeerName Lib.PeerRequest
             -> STMRespChanMap PeerID Lib.PeerResponse
             -> [ProcessorMessage]
             -> STM.STM ()
sendMessages clients peers responsePeers toSend = do
            forM_ toSend $ \msg' -> do
                case msg' of
                    Reply clientId reply -> sendTo clients clientId reply
                    PeerRequest peerName req -> sendTo peers peerName req
                    PeerReply peerId req -> sendTo responsePeers peerId req
    where
        sendTo mapping xid msg = do
            queuep <- Map.lookup xid <$> STM.readTVar mapping
            case queuep of
                Just q -> STM.writeTQueue q $ Just msg
                Nothing -> error "what?"



processClientRequestMessage :: ClientID -> Lib.ClientRequest -> ProtoStateMachine
processClientRequestMessage sender Lib.Bing = do
    s <- bings <$>  RWS.get
    peers <- RWS.asks peerNames

    case listToMaybe $ drop (if length peers > 0 then (s `mod` length peers) else 0) peers of
        Just aPeerName -> do
            RWS.tell [PeerRequest aPeerName $ Lib.IHave s]
            RWS.modify $ \st -> st {
                bings = 1 + bings st,
                pending = Map.insert s (Reply sender $ Lib.Bong s) $ pending st
            }
        Nothing -> do
            RWS.tell [Reply sender $ Lib.Bong s]


processClientRequestMessage sender Lib.Ping = do
    s <- bings <$> RWS.get
    RWS.tell [Reply sender $ Lib.Bong s]
    RWS.modify $ \st -> st { bings = 1 - bings st }


processPeerRequestMessage :: PeerID -> Lib.PeerRequest -> ProtoStateMachine
processPeerRequestMessage sender (Lib.IHave n) = do
    RWS.tell [PeerReply sender $ Lib.ThatsNiceDear n]

processPeerResponseMessage :: PeerName -> Lib.PeerResponse -> ProtoStateMachine
processPeerResponseMessage _sender (Lib.ThatsNiceDear n) = do
    -- error $"processPeerResponseMessage" ++ show (_sender, resp)

    toSend <- Map.lookup n . pending <$> RWS.get
    RWS.modify $ \st -> st {
        pending = Map.delete n $ pending st
    }

    RWS.tell $ maybe [] return toSend

processTick :: () -> Tick -> ProtoStateMachine
processTick () (Tick _t) = do
    -- toSend <- pending <$> RWS.get
    -- RWS.modify $ \st -> st { pending = [] }
    -- RWS.tell toSend
    return ()