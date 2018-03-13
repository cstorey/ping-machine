module Server where

import qualified Lib

import qualified Network.Socket            as S
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import qualified Data.Binary as Binary
import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Data.IORef as IORef

-- import qualified Data.ByteString as B

import qualified Control.Concurrent as C
import Control.Monad
import qualified Control.Exception as E

main :: IO ()
main = S.withSocketsDo $ do
    addr <- resolve "3000"
    E.bracket (listenFor addr) S.close accepter
    return ()

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

accepter :: S.Socket -> IO ()
accepter listener = do
    stateRef <- IORef.newIORef 0
    void $ forever $ do
        (client, x) <- S.accept listener
        putStrLn $ show (client, x)
        C.forkIO $ do
            E.bracket (streamsOf client) (const $ S.close client) (handleClient stateRef)

streamsOf :: (Binary.Binary a, Binary.Binary b) => S.Socket -> IO (Streams.InputStream a, Streams.OutputStream b)
streamsOf client = do
    (is, os) <- (Streams.socketToStreams client)
    (,) <$> BStreams.decodeInputStream is <*> BStreams.encodeOutputStream os

handleClient :: IORef.IORef Int -> (Streams.InputStream Lib.Message, Streams.OutputStream Lib.Message) -> IO ()
handleClient ref (is,os) = go
    where
    go = do
        it <- Streams.read is
        case it of
            Just msg -> do
                putStrLn $ "<- " ++ show msg
                resps <- IORef.atomicModifyIORef ref $ 
                    \s -> let ((), s', resps) = RWS.runRWS (processMessage msg) () s in (s', resps)
                putStrLn $ "-> " ++ show resps
                forM_ resps $ \resp -> Streams.write (Just resp) os
                go
            Nothing -> Streams.write Nothing os

processMessage :: Lib.Message -> RWS.RWS () [Lib.Message] Int ()
processMessage Lib.Bing = do
    s <- RWS.get
    RWS.tell [Lib.Bong s]
    RWS.modify (+1)
processMessage (Lib.Bong _) = do
    RWS.tell [Lib.Bing]