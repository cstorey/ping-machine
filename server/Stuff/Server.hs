{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.Server where

import qualified Stuff.Proto as Proto

import           System.IO (BufferMode(..), hSetBuffering, stdout, stderr)
import qualified Network.Socket            as S
import qualified System.Environment as Env
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.Async as Async
import qualified Data.Map as Map
import qualified Data.Time.Clock.POSIX as Clock
import System.Random as Random
import qualified Control.Concurrent as C
import Control.Monad

import Stuff.Types
import Stuff.Network
import Stuff.RaftDriver

now :: IO Time
now = fromRational . toRational <$> Clock.getPOSIXTime

main ::  IO ()
main = S.withSocketsDo $ do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering

    clientPort : peerPort : peerPorts <- Env.getArgs
    let myName = Proto.PeerName peerPort
    clientAddr <- resolve clientPort
    peerListenAddr <- resolve peerPort
    clientReqQ <- STM.atomically STM.newTQueue:: IO (RequestsQ Proto.ClientRequest Proto.ClientResponse)
    peerReqInQ <- STM.atomically STM.newTQueue :: IO (RequestsQ Proto.PeerRequest Proto.PeerResponse)
    peerRespQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (m ()))
    ticks <- STM.atomically STM.newTQueue :: IO (STM.TQueue ((),Maybe Tick))
    requestToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STMReqChanMap Proto.PeerName Proto.PeerRequest Proto.PeerResponse (m ()))
    -- We also need to start a peer manager. This will start a single process
    -- for each known peer, attempt to connect, then relay messages to/from
    -- peers.
    let race = Async.race_
    withTicker ticks $ \ticker ->
        (runReqRespListener (ClientID <$> nextId) clientAddr clientReqQ)
            `race` (runReqRespListener (PeerID <$> nextId) peerListenAddr peerReqInQ)
            `race` (runOutgoing (Proto.PeerName <$> peerPorts) requestToPeers peerRespQ)
            `race` (Async.wait $ waiter ticker)
            `race` (runModel myName clientReqQ peerReqInQ peerRespQ ticks requestToPeers )

nextId :: IO Int
nextId = STM.atomically nextIdSTM

data Ticker = Ticker {
    waiter :: Async.Async ()
}

withTicker :: STM.TQueue ((), Maybe Tick) -> (Ticker  -> IO a) -> IO a
withTicker ticks f = Async.withAsync (runTicker ticks) $ \ticker -> f $ Ticker ticker

runTicker :: STM.TQueue ((), Maybe Tick) -> IO ()
runTicker ticks = void $ forever $ do
    t <- now
    STM.atomically $ STM.writeTQueue ticks ((), Just $ Tick t)
    putStrLn $ "Tick: " ++ show t
    sleepTime <- Random.getStdRandom $ Random.randomR (oneSec `div` 2, oneSec * 3 `div` 2 )
    C.threadDelay sleepTime
    where
    oneSec = 1000000
