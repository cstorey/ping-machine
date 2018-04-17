{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.Server where

import qualified Stuff.Proto as Proto
import qualified Stuff.Models as Models

import           System.IO (BufferMode(..), hSetBuffering, stdout, stderr)
import qualified Network.Socket            as S
import qualified System.Environment as Env
import qualified Control.Concurrent.STM as STM
import qualified Data.Map as Map

import Stuff.Types
import Stuff.Network
import Stuff.RaftDriver
import Stuff.Ticker

main ::  IO ()
main = S.withSocketsDo $ do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering

    clientPort : peerPort : peerPorts <- Env.getArgs
    let myName = Proto.PeerName peerPort
    clientAddr <- resolve clientPort
    peerListenAddr <- resolve peerPort
    peerRespQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (m ()))
    requestToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STMReqChanMap Proto.PeerName (Proto.PeerRequest Models.BingBongReq) Proto.PeerResponse (m ()))

    withTicker $ \ticker ->
      withReqRespListener (ClientID <$> nextId) clientAddr $ \clientListener -> do
        withReqRespListener (PeerID <$> nextId) peerListenAddr $ \peerListener -> do
          withOutgoing (Proto.PeerName <$> peerPorts) requestToPeers peerRespQ $ \_outgoing -> do
            (runModel myName clientListener (listenerRequests peerListener) peerRespQ ticker requestToPeers Models.bingBongModel (0 :: Int))

nextId :: IO Int
nextId = STM.atomically nextIdSTM

