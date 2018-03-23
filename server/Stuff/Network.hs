module Stuff.Network where

import qualified Lib

import qualified Network.Socket            as S
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import qualified Data.Binary as Binary
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.Async as Async
import qualified Data.Map as Map
import qualified Debug.Trace as Trace
import Data.List ((\\))
import qualified Control.Concurrent as C
import Control.Monad
import qualified Control.Exception as E

import Stuff.Types

oneSecondMicroSeconds :: Int
oneSecondMicroSeconds = 1000000

-- Supervisor
runOutgoing :: [Lib.PeerName]
            -> STMRespChanMap Lib.PeerName Lib.PeerRequest
            -> RequestsInQ Lib.PeerName Lib.PeerResponse
            -> IO ()
runOutgoing seedPeers peers peerRespQ = do
    putStrLn $ "peers:" ++ show seedPeers
    processes <- STM.atomically $ STM.newTVar $ Map.empty

    void $ forever $ do
        runningProcesses <- Map.elems <$> STM.readTVarIO processes
        let toStart = (seedPeers \\ runningProcesses)
        putStrLn $ "To start: " ++ show toStart
        forM_ toStart $ \name -> do
            q <- STM.newTQueueIO
            putStrLn $ "Starting: " ++ show name
            a <- Async.async $ runPeer peerRespQ name q
            STM.atomically $ do
                STM.modifyTVar peers $ Map.insert name q
                STM.modifyTVar processes $ Map.insert a name

        do
            procNames <- Map.elems <$> STM.readTVarIO processes
            putStrLn $ "procs:" ++ show procNames

        (peerNamep, ret) <- STM.atomically $ do
            procs <- STM.readTVar processes
            (a, retOrError) <- Async.waitAnyCatchSTM $ Map.keys procs
            let namep = Map.lookup a procs
            STM.writeTVar processes $ Map.delete a procs
            return (namep, retOrError)

        putStrLn $ "runOutgoing failed process: " ++ show (peerNamep, ret)
        C.threadDelay $ oneSecondMicroSeconds
        putStrLn $ "runOutgoing : restarting"

connect :: S.AddrInfo -> IO S.Socket
connect addr = do
    sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    Trace.trace ("Connecting " ++ show sock ++ " to " ++ show addr) $ return ()
    (S.connect sock $ S.addrAddress addr) `E.onException` do
        Trace.trace ("Failed " ++ show sock ++ " to "  ++ show addr) $ return ()
        S.close sock
    Trace.trace ("Connected " ++ show sock ++ " to "  ++ show addr) $ return ()
    return sock

runPeer :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp) => RequestsInQ Lib.PeerName req -> Lib.PeerName -> ResponsesOutQ resp -> IO ()
runPeer fromPeerQ name toPeerQ = do
    Trace.trace ("lookup peer " ++ show name) $ return ()
    addrInfo <- resolve $ Lib.unPeerName name
    Trace.trace ("initiate open " ++ show name) $ return ()
    (is, os) <- streamsOf =<< connect addrInfo

    Trace.trace ("Talking to peer " ++ show name) $ return ()
    processClientRequests fromPeerQ name toPeerQ (is, os)
    Trace.trace ("Finished with peer " ++ show name) $ return ()

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
    Async.concurrently_ (reader_ clientId inQ) (writer_ inQ)
    putStrLn $ "Done: " ++ show clientId
    where
    reader_ senderId writerQ = do
        it <- Streams.read is
        case it of
            Just msg -> do
                when False $ putStrLn $ "<- " ++ show clientId ++ ":" ++ show msg
                STM.atomically $ STM.writeTQueue outQ $ (senderId, Just (msg))
                reader_ senderId writerQ
            Nothing -> STM.atomically $ STM.writeTQueue writerQ Nothing

    writer_ sender =  do
        msg <- STM.atomically $ STM.readTQueue sender
        when False $ putStrLn $ "-> " ++ show clientId ++ ":" ++ show msg
        Streams.write msg os
        maybe (return ()) (const $ writer_ sender) msg

