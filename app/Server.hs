module Server where

import qualified Lib

import qualified Network.Socket            as S
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import qualified Data.Binary as Binary
import qualified Control.Monad.Trans.RWS.Strict as RWS

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
    void $ forever $ do
        (client, x) <- S.accept listener
        putStrLn $ show (client, x)
        C.forkIO $ do
            E.bracket (streamsOf client) (const $ S.close client) handleClient

streamsOf :: (Binary.Binary a, Binary.Binary b) => S.Socket -> IO (Streams.InputStream a, Streams.OutputStream b)
streamsOf client = do
    (is, os) <- (Streams.socketToStreams client)
    (,) <$> BStreams.decodeInputStream is <*> BStreams.encodeOutputStream os

handleClient :: (Streams.InputStream Lib.Message, Streams.OutputStream Lib.Message)  -> IO ()
handleClient (is,os) = do
    it <- Streams.read is
    case it of
        Just msg -> do
            putStrLn $ "<- " ++ show msg
            let ((), (), resps) = RWS.runRWS (processMessage msg) () ()
            putStrLn $ "-> " ++ show resps
            forM_ resps $ \resp -> Streams.write (Just resp) os
            handleClient (is,os)
        Nothing -> Streams.write Nothing os

processMessage :: Lib.Message -> RWS.RWS () [Lib.Message] () ()
processMessage Lib.Bing = do
    RWS.tell [Lib.Bong]
processMessage Lib.Bong = do
    RWS.tell [Lib.Bing]