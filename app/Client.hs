{-# LANGUAGE OverloadedStrings #-}

module Client where

import qualified Lib

import qualified Control.Exception as E
import Network.Socket hiding (recv)
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import qualified Data.Binary as Binary
import qualified System.Environment as Env

main :: IO ()
main = withSocketsDo $ do
    [sport] <- Env.getArgs
    addr <- resolve "127.0.0.1" sport
    E.bracket (open addr) close talk

resolve :: HostName -> ServiceName -> IO AddrInfo
resolve host port = do
    let hints = defaultHints { addrSocketType = Stream }
    addr:_ <- getAddrInfo (Just hints) (Just host) (Just port)
    return addr

open :: AddrInfo -> IO Socket
open addr = do
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    connect sock $ addrAddress addr
    return sock

streamsOf :: (Binary.Binary a, Binary.Binary b) => Socket -> IO (Streams.InputStream a, Streams.OutputStream b)
streamsOf client = do
    (is, os) <- (Streams.socketToStreams client)
    eis <- BStreams.decodeInputStream is
    eos <- BStreams.encodeOutputStream os

    return (eis, eos)

talk :: Socket -> IO ()
talk sock = do
    (is, os) <- streamsOf sock
    go is os

go :: Streams.InputStream Lib.ClientResponse -> Streams.OutputStream Lib.ClientRequest -> IO ()
go is os = do
    Streams.write (Just Lib.Bing) os
    putStrLn "Sent"
    msg <- Streams.read is
    putStr "Received: "
    putStrLn $ show msg
    case msg of
        -- Just (Right _) -> go is os
        _ -> return ()
