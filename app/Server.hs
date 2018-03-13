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

-- import qualified Data.ByteString as B

import qualified Control.Concurrent as C
import Control.Monad
import qualified Control.Exception as E

type ResponsesQ resp =  STM.TQueue (Maybe resp)
type RequestsQ req resp =  STM.TQueue (ResponsesQ resp, Maybe req)

main :: IO ()
main = S.withSocketsDo $ do
    myPort : _others <- Env.getArgs
    addr <- resolve myPort
    modelQ <- STM.atomically STM.newTQueue
    Async.race_ (runModel modelQ) $ E.bracket (listenFor addr) S.close (runAcceptor modelQ)

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

runAcceptor :: (Show req, Binary.Binary req, Show resp, Binary.Binary resp) => RequestsQ req resp -> S.Socket  -> IO ()
runAcceptor modelQ listener = do
        void $ forever $ do
            (client, x) <- S.accept listener
            sender <- STM.atomically $ STM.newTQueue
            putStrLn $ show (client, x)
            C.forkIO $ do
                E.bracket (streamsOf client) (const $ S.close client) (handleConn modelQ sender)

handleConn :: (Show req, Show resp)
            => RequestsQ req resp
            -> ResponsesQ resp
            -> (Streams.InputStream req, Streams.OutputStream resp)
            -> IO ()
handleConn modelQ sender (is,os) = do
    Async.concurrently_ reader writer
    putStrLn "Client done"
    where
    reader = do
        it <- Streams.read is
        case it of
            Just msg -> do
                putStrLn $ "<- " ++ show msg
                STM.atomically $ STM.writeTQueue modelQ $ (sender, Just (msg))
                reader
            Nothing -> STM.atomically $ STM.writeTQueue sender Nothing

    writer =  do
        msg <- STM.atomically $ STM.readTQueue sender
        putStrLn $ "-> " ++ show msg
        case msg of
            Just m -> do
                Streams.write (Just m) os
                writer
            Nothing -> do
                Streams.write Nothing os
                return ()

--- Model bits

runModel :: RequestsQ Lib.ClientRequest Lib.ClientResponse -> IO ()
runModel modelQ = do
    stateRef <- STM.atomically $ STM.newTVar 0
    go stateRef
    where
    go stateRef = do
        STM.atomically $ do
            (sender, m) <- STM.readTQueue modelQ
            case m of
                Just msg -> do
                    s <- STM.readTVar stateRef
                    let ((), s', resps) = RWS.runRWS (processMessage msg) () s
                    STM.writeTVar stateRef s'
                    forM_ resps $ STM.writeTQueue sender . Just
                Nothing -> return ()
        go stateRef

processMessage :: Lib.ClientRequest -> RWS.RWS () [Lib.ClientResponse] Int ()
processMessage Lib.Bing = do
    s <- RWS.get
    RWS.tell [Lib.Bong s]
    RWS.modify (+1)

processMessage Lib.Ping = do
    s <- RWS.get
    RWS.tell [Lib.Bong s]
    RWS.modify (flip (-) 1)