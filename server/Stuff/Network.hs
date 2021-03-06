{-# LANGUAGE ScopedTypeVariables #-}
module Stuff.Network
( withOutgoing
, withReqRespListener
, resolve
, Listener(..)
)
where

import qualified Stuff.Proto as Proto

import qualified Network.Socket            as S
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import Data.Binary (Binary)
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
import Stuff.RaftDriver (Listener(..), Outgoing(..))

oneSecondMicroSeconds :: Int
oneSecondMicroSeconds = 1000000


-- Supervisor
withOutgoing :: forall req resp a . (Binary req, Show req)
            => [Proto.PeerName]
            -> (Outgoing req resp -> IO a)
            -> IO a
withOutgoing seedPeers rest = do
    -- "Responses"
    peerRespQ <- STM.atomically STM.newTQueue
    -- Requests from the model to the outside world
    peers <- STM.newTVarIO $ Map.empty

    putStrLn $ "peers:" ++ show seedPeers
    processes <- STM.atomically $ STM.newTVar $ Map.empty
    let outgoing = Outgoing peers peerRespQ
    Async.withAsync (go outgoing processes) $ \tid -> do
      Async.link tid
      rest $ outgoing

    where
    go :: Outgoing req resp -> STM.TVar (Map.Map (Async.Async ()) Proto.PeerName) -> IO ()
    go (Outgoing peers peerRespQ) processes = void $ forever $ do
        runningProcesses <- Map.elems <$> STM.readTVarIO processes
        let toStart = (seedPeers \\ runningProcesses)
        putStrLn $ "To start: " ++ show toStart
        forM_ toStart $ \name -> do
            q <- STM.newTQueueIO
            putStrLn $ "Starting: " ++ show name
            a <- Async.async $ runPeer q peerRespQ name
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

runPeer :: forall req resp . (Binary.Binary req, Show req, Binary.Binary resp, Show resp)
        => OutgoingReqQ req resp -> STM.TQueue (Proto.PeerName, resp) -> Proto.PeerName -> IO ()
runPeer toPeerQ fromPeerQ name = do
    Trace.trace ("lookup peer " ++ show name) $ return ()
    addrInfo <- resolve $ Proto.unPeerName name
    Trace.trace ("initiate open " ++ show name) $ return ()
    (is, os) <- streamsOf =<< connect addrInfo :: IO (Streams.InputStream resp, Streams.OutputStream req)

    Trace.trace ("Talking to peer " ++ show name) $ return ()
    processOutgoingConnection toPeerQ fromPeerQ name (is, os)
    Trace.trace ("Finished with peer " ++ show name) $ return ()

resolve :: S.ServiceName -> IO S.AddrInfo
resolve port = do
    let hints = S.defaultHints {
            S.addrFlags = [S.AI_PASSIVE]
        , S.addrSocketType = S.Stream
        }
    addr:_ <- S.getAddrInfo (Just hints) (Just "127.0.0.1") (Just port)
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

processOutgoingConnection :: (Show xid, Show req, Show resp) =>
       OutgoingReqQ req resp
    -> STM.TQueue (xid, resp)
    -> xid
    -> (Streams.InputStream resp, Streams.OutputStream req)
    -> IO ()

processOutgoingConnection reqQ respQ clientId (is, os) = do
    pendingResponses <- STM.newTQueueIO
    Async.concurrently_ (requests pendingResponses) (responses pendingResponses)
    putStrLn $ "Done: " ++ show clientId
    where
    requests pendingResponses = do
        msg <- STM.atomically $ do
          msg <- STM.readTQueue reqQ
          return msg

        when False $ putStrLn $ "-> " ++ show clientId ++ ":" ++ show msg

        Streams.write (Just msg) os
        requests pendingResponses

    responses pendingResponses = do
        it <- Streams.read is
        case it of
            Just msg -> do
                when False $ putStrLn $ "<- " ++ show clientId ++ ":" ++ show msg
                STM.atomically $ do
                  STM.writeTQueue respQ (clientId, msg)
                responses pendingResponses
            Nothing -> error $ "Well, I'm done: " ++ show clientId


withReqRespListener :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp, Show xid)
    => IO xid
    -> S.AddrInfo
    -> (Listener req resp -> IO a)
    -> IO a
withReqRespListener newId addr action =
    E.bracket (listenFor addr) S.close $ \sock -> do
      reqs <- STM.newTQueueIO
      Async.withAsync (go reqs sock) $ \tid -> do
        Async.link tid
        action $ Listener reqs
    where
    go reqs = runAcceptor newId $ handleReqRespConn reqs

handleReqRespConn :: (Show req, Show resp, Show xid)
            => RequestsQ req resp
            -> xid
            -> (Streams.InputStream req, Streams.OutputStream resp)
            -> IO ()

handleReqRespConn modelQ clientId (is,os) = do
    processReqRespConn modelQ clientId (is, os)

processReqRespConn :: (Show xid, Show req, Show resp) =>
       RequestsQ req resp
    -> xid
    -> (Streams.InputStream req, Streams.OutputStream resp)
    -> IO ()

processReqRespConn outQ clientId (is, os) = do
    pendingResponses <- STM.newTQueueIO :: IO (STM.TQueue (Maybe (STM.TMVar resp)))
    Async.concurrently_ (reader_ clientId pendingResponses) (writer_ pendingResponses)
    putStrLn $ "Done: " ++ show clientId
    where
    reader_ senderId pendingResponses = do
        it <- Streams.read is
        case it of
            Just msg -> do
                when False $ putStrLn $ "<- " ++ show clientId ++ ":" ++ show msg
                STM.atomically $ do
                  respVar <- STM.newEmptyTMVar
                  STM.writeTQueue outQ (msg, respVar)
                  STM.writeTQueue pendingResponses $ Just respVar
                reader_ senderId pendingResponses
            Nothing -> STM.atomically $ STM.writeTQueue pendingResponses Nothing

    -- writer_ :: STM.TQueue (Maybe (STM.TMVar _)) -> IO ()
    writer_ pendingResponses =  do
        msg <- STM.atomically $ do
          respp <- STM.readTQueue pendingResponses
          msg <- case respp of
            Just respVar -> fmap Just $ STM.takeTMVar respVar
            Nothing -> return Nothing
          -- msg <- maybe (return Nothing) (Just <$> STM.readTMVar) respp
          return msg
        when False $ putStrLn $ "-> " ++ show clientId ++ ":" ++ show msg
        Streams.write msg os
        maybe (return ()) (const $ writer_ pendingResponses) msg
