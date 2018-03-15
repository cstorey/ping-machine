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
import Control.Applicative ((<|>))


-- import qualified Data.ByteString as B

import qualified Control.Concurrent as C
import Control.Monad
import qualified Control.Exception as E

type ResponsesQ resp =  STM.TQueue (Maybe resp)
type RequestsInQ sender req =  STM.TQueue (sender, Maybe req)


newtype ClientID = ClientID Int
    deriving (Show, Eq, Ord)

-- Identifier for an _incoming_ request
newtype PeerID = PeerID Int
    deriving (Show, Eq, Ord)


-- Identifier for an outgoing request
newtype PeerName = PeerName String
    deriving (Show, Eq)

data Tick = Tick

data ProtocolState = ProtocolState {
    bings :: Int,
    pending :: [ProcessorMessage]
}

main ::  IO ()
main = S.withSocketsDo $ do
    clientPort : peerPort : peerPorts <- Env.getArgs
    clientAddr <- resolve clientPort
    peerListenAddr <- resolve peerPort
    ids <- STM.atomically $ STM.newTVar 0
    clientReqQ <- STM.atomically STM.newTQueue:: IO (STM.TQueue (ClientID,Maybe Lib.ClientRequest))
    peerInQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerID,Maybe Lib.PeerRequest))
    peerOutQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerName,Maybe Lib.PeerRequest))
    ticks <- STM.atomically STM.newTQueue :: IO (STM.TQueue ((),Maybe Tick))
    clients <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map ClientID (ResponsesQ Lib.ClientResponse)))
    incomingPeers <- STM.atomically $ STM.newTVar $ Map.empty  :: IO (STM.TVar (Map.Map PeerID (ResponsesQ Lib.PeerRequest)))
    -- We also need to start a peer manager. This will start a single process
    -- for each known peer, attempt to connect, then relay messages to/from
    -- peers.
    let race = Async.race_
    (runListener (ClientID <$> nextId ids) clientAddr clients clientReqQ) `race`
        (runListener (PeerID <$> nextId ids) peerListenAddr incomingPeers peerInQ) `race`
        (runOutgoing (PeerName <$> peerPorts) peerOutQ) `race`
        (runTicker ticks) `race`
        (runModel clientReqQ peerInQ clients ticks peerOutQ)

nextId :: STM.TVar Int -> IO Int
nextId ids = STM.atomically $ do
    n <- STM.readTVar ids
    STM.writeTVar ids (n+1)
    return n


runOutgoing :: [PeerName] -> STM.TQueue (PeerName, Maybe Lib.PeerRequest) -> IO ()
runOutgoing _peers msgs = do
    void $ forever $ STM.atomically $ do
        (n, msg) <- STM.readTQueue msgs
        error $ "runOutgoing" ++ show (n, msg)

runListener :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp, Show xid, Ord xid)
            => IO xid
            -> S.AddrInfo
            -> STM.TVar (Map.Map xid (ResponsesQ resp))
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
            => STM.TVar (Map.Map xid (ResponsesQ resp))
            -> RequestsInQ xid req
            -> xid
            -> (Streams.InputStream req, Streams.OutputStream resp)
            -> IO ()

handleConn clients modelQ clientId (is,os) = do
    q <- STM.atomically $ do
        q <- STM.newTQueue
        STM.modifyTVar clients $ Map.insert clientId q
        return q

    Async.concurrently_ (reader clientId q) (writer q)
    putStrLn "Client done"
    where
    reader senderId writerQ = do
        it <- Streams.read is
        case it of
            Just msg -> do
                putStrLn $ "<- " ++ show clientId ++ ":" ++ show msg
                STM.atomically $ STM.writeTQueue modelQ $ (senderId, Just (msg))
                reader senderId writerQ
            Nothing -> STM.atomically $ STM.writeTQueue writerQ Nothing

    writer sender =  do
        msg <- STM.atomically $ STM.readTQueue sender
        putStrLn $ "-> " ++ show clientId ++ ":" ++ show msg
        case msg of
            Just m -> do
                Streams.write (Just m) os
                writer sender
            Nothing -> do
                Streams.write Nothing os
                return ()


runTicker :: STM.TQueue ((), Maybe Tick) -> IO ()
runTicker ticks = void $ forever $ do
    STM.atomically $ STM.writeTQueue ticks ((), Just Tick)
    putStrLn "Tick"
    C.threadDelay 1000000

--- Model bits

data MessageSend clientId reply dest peerReq = Reply clientId reply
    | PeerMessage dest peerReq

type STMRespChanMap xid resp = STM.TVar (Map.Map xid (ResponsesQ resp))

data Hole

runModel :: RequestsInQ ClientID Lib.ClientRequest
            -> RequestsInQ PeerID Lib.PeerRequest
            -> STMRespChanMap ClientID Lib.ClientResponse
            -> RequestsInQ () Tick
            -> RequestsInQ PeerName Lib.PeerRequest
            -> IO ()
runModel modelQ _peerReqsQ clients ticks peerOutQ = do
    stateRef <- STM.atomically $ STM.newTVar $ ProtocolState 0 []

    let processClientMessage = processMessageSTM stateRef modelQ processMessage
    let processTickMessage = processMessageSTM stateRef ticks processTick
    let processPeerMessage = STM.retry

    forever $ STM.atomically $ do
        outputs <- processClientMessage <|> processPeerMessage <|> processTickMessage
        sendMessages clients peerOuts outputs

processMessageSTM :: STM.TVar ProtocolState
                  -> RequestsInQ xid req
                  -> (xid -> req -> RWS.RWS () [MessageSend a b c d] ProtocolState ())
                  -> STM.STM [MessageSend a b c d]
processMessageSTM stateRef reqQ process = do
    (sender, m) <- STM.readTQueue reqQ
    case m of
        Just msg -> do
            s <- STM.readTVar stateRef
            let ((), s', toSend) = RWS.runRWS (process sender msg) () s
            STM.writeTVar stateRef s'
            return toSend
        Nothing -> return []

sendMessages :: STMRespChanMap ClientID Lib.ClientResponse
             -> STMRespChanMap PeerID Lib.PeerRequest
             -> [ProcessorMessage]
             -> STM.STM ()
sendMessages clients peers toSend = do
            forM_ toSend $ \msg' -> do
                case msg' of
                    Reply clientId reply -> sendTo clients clientId reply
                    PeerMessage peerId req -> sendTo peers peerId req
    where
        sendTo mapping xid msg = do
            queuep <- Map.lookup xid <$> STM.readTVar mapping
            case queuep of
                Just q -> STM.writeTQueue q $ Just msg
                Nothing -> error "what?"



type ProcessorMessage = MessageSend ClientID Lib.ClientResponse PeerID Lib.PeerRequest

processMessage :: ClientID -> Lib.ClientRequest -> RWS.RWS () [ProcessorMessage] ProtocolState ()
processMessage sender Lib.Bing = do
    s <- bings <$>  RWS.get
    RWS.modify $ \st -> st {
        bings = 1 + bings st,
        pending = pending st ++ [Reply sender $ Lib.Bong s]
    }

processMessage sender Lib.Ping = do
    s <- bings <$> RWS.get
    RWS.tell [Reply sender $ Lib.Bong s]
    RWS.modify $ \st -> st { bings = 1 - bings st }

processTick :: () -> Tick -> RWS.RWS () [ProcessorMessage] ProtocolState ()
processTick () Tick = do
    toSend <- pending <$> RWS.get
    RWS.modify $ \st -> st { pending = [] }
    RWS.tell toSend