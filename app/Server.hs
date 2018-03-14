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
type RequestsQ sender req =  STM.TQueue (sender, Maybe req)


newtype ClientID = ClientID Int
    deriving (Show, Eq, Ord)

-- Newtype for Int, eventually
data PeerID

main :: IO ()
main = S.withSocketsDo $ do
    clientPort : _peerPort : _peers <- Env.getArgs
    clientAddr <- resolve clientPort
    -- peerAddr <- resolve peerPort
    clientReqQ <- STM.atomically STM.newTQueue:: IO (STM.TQueue (ClientID,Maybe Lib.ClientRequest))
    peerReqQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerID,Maybe Lib.PeerRequest))
    clients <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map ClientID (ResponsesQ Lib.ClientResponse)))
    -- peers <- STM.atomically $ STM.newTVar $ Map.empty
    -- We also need to start a peer manager. This will start a single process
    -- for each known peer, attempt to connect, then relay messages to/from
    -- peers.
    Async.withAsync (runListener ClientID clientAddr clients clientReqQ) $ \_a0 -> do
        -- Async.withAsync (runListener peerAddr peers peerReqQ) $ \_a0 -> do
            (runModel clientReqQ peerReqQ clients processMessage)

runListener ::  (Binary.Binary req, Show req, Binary.Binary resp, Show resp, Show xid, Ord xid)
            => (Int -> xid)
            -> S.AddrInfo
            -> STM.TVar (Map.Map xid (ResponsesQ resp))
            -> RequestsQ xid req
            -> IO ()
runListener toid addr clients reqs =
    E.bracket (listenFor addr) S.close (runAcceptor toid $ handleConn clients reqs)


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
            => (Int -> xid)
            -> (xid -> (Streams.InputStream req, Streams.OutputStream resp) -> IO ())
            -> S.Socket
            -> IO ()
runAcceptor toid handler listener = do
    go 0
    where
    go n = do
        (client, x) <- S.accept listener
        putStrLn $ show (client, x)
        _ <- C.forkIO $ do
            E.bracket (streamsOf client) (const $ S.close client) (handler $ toid n)
        go (n+1)

handleConn :: (Show req, Show resp, Show xid, Ord xid)
            => STM.TVar (Map.Map xid (ResponsesQ resp))
            -> RequestsQ xid req
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

--- Model bits

runModel :: Ord xid
            => RequestsQ xid req
            -> RequestsQ preq presp
            -> STM.TVar (Map.Map xid (ResponsesQ resp))
            -> (req -> RWS.RWS () [resp] Int ())
            -> IO ()
runModel modelQ _peerReqsQ clients process = do
    stateRef <- STM.atomically $ STM.newTVar 0
    let processClientMessage = do
            (clientId, m) <- STM.readTQueue modelQ
            case m of
                Just msg -> do
                    s <- STM.readTVar stateRef
                    let ((), s', resps) = RWS.runRWS (process msg) () s
                    STM.writeTVar stateRef s'
                    clientp <- Map.lookup clientId <$> STM.readTVar clients
                    case clientp of
                        Just q -> forM_ resps $ STM.writeTQueue q . Just
                        Nothing -> error "what?"
                Nothing -> return ()

    let processPeerMessage = STM.retry

    forever $ STM.atomically $ processClientMessage <|> processPeerMessage

processMessage :: Lib.ClientRequest -> RWS.RWS () [Lib.ClientResponse] Int ()
processMessage Lib.Bing = do
    s <- RWS.get
    RWS.tell [Lib.Bong s]
    RWS.modify (+1)

processMessage Lib.Ping = do
    s <- RWS.get
    RWS.tell [Lib.Bong s]
    RWS.modify (flip (-) 1)

{-
 * Track ClientIDs and make response targets explicit
 * Acceptor adds client ids to a map in STM
 * Add a Tick event
 * Delay responses until next Tick
-}