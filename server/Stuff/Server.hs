{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.Server where

import qualified Lib

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
import Stuff.RaftModel

now :: IO Time
now = fromRational . toRational <$> Clock.getPOSIXTime

main ::  IO ()
main = S.withSocketsDo $ do
    clientPort : peerPort : peerPorts <- Env.getArgs
    let myName = Lib.PeerName peerPort
    clientAddr <- resolve clientPort
    peerListenAddr <- resolve peerPort
    ids <- STM.atomically $ STM.newTVar 0
    clientReqQ <- STM.atomically STM.newTQueue:: IO (STM.TQueue (ClientID,Maybe Lib.ClientRequest))
    peerReqInQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerID,Maybe Lib.PeerRequest))
    peerRespQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (Lib.PeerName,Maybe Lib.PeerResponse))
    ticks <- STM.atomically STM.newTQueue :: IO (STM.TQueue ((),Maybe Tick))
    clients <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map ClientID (ResponsesOutQ Lib.ClientResponse)))
    requestToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map Lib.PeerName (ResponsesOutQ Lib.PeerRequest)))
    responsesToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map PeerID (ResponsesOutQ Lib.PeerResponse)))
    -- We also need to start a peer manager. This will start a single process
    -- for each known peer, attempt to connect, then relay messages to/from
    -- peers.
    let race = Async.race_
    (runListener (ClientID <$> nextId ids) clientAddr clients clientReqQ)
        `race` (runListener (PeerID <$> nextId ids) peerListenAddr responsesToPeers peerReqInQ)
        `race` (runOutgoing (Lib.PeerName <$> peerPorts) requestToPeers peerRespQ)
        `race` (runTicker ticks)
        `race` (runModel myName clientReqQ peerReqInQ peerRespQ clients ticks requestToPeers responsesToPeers)

nextId :: STM.TVar Int -> IO Int
nextId ids = STM.atomically $ do
    n <- STM.readTVar ids
    STM.writeTVar ids (n+1)
    return n

runTicker :: STM.TQueue ((), Maybe Tick) -> IO ()
runTicker ticks = void $ forever $ do
    t <- now
    STM.atomically $ STM.writeTQueue ticks ((), Just $ Tick t)
    putStrLn $ "Tick: " ++ show t
    sleepTime <- Random.getStdRandom $ Random.randomR (oneSec `div` 2, oneSec * 3 `div` 2 )
    C.threadDelay sleepTime
    where
    oneSec = 1000000