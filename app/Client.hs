{-# LANGUAGE OverloadedStrings #-}

module Client where

import qualified Lib

import qualified Control.Exception as E
import Network.Socket hiding (recv)
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams

main :: IO ()
main = withSocketsDo $ do
    addr <- resolve "127.0.0.1" "3000"
    E.bracket (open addr) close talk
  where
    resolve host port = do
        let hints = defaultHints { addrSocketType = Stream }
        addr:_ <- getAddrInfo (Just hints) (Just host) (Just port)
        return addr
    open addr = do
        sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
        connect sock $ addrAddress addr
        return sock

    streamsOf client = do
        (is, os) <- (Streams.socketToStreams client)
        eis <- BStreams.decodeInputStream is
        eos <- BStreams.encodeOutputStream os

        return (eis, eos)
    talk sock = do
        (is, os) <- streamsOf sock
        Streams.write (Just Lib.Bing) os
        go is os
    go is os = do
        msg <- (Streams.read is :: IO (Maybe Lib.Message))
        putStr "Received: "
        putStrLn $ show msg
        case msg of
            Just x -> do
                Streams.write (Just x) os
                go is os
            Nothing -> return ()
