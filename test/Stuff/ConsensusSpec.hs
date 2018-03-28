{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Stuff.ConsensusSpec (spec) where

import Test.Hspec

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Control.Monad
import Data.Sequence (Seq, (|>))
import qualified Data.Sequence as Seq
import Data.Foldable (traverse_)
import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Monad.Trans.State.Strict as State
-- import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..), get)
-- import           Control.Monad.Reader.Class (MonadReader(..), asks)
import Control.Monad.Logger

import Lens.Micro.Platform

import Stuff.RaftModel
import Stuff.Proto
import Stuff.Types

newtype WaitingCallback = WaitingCallback (PeerResponse -> ProtoStateMachine ())

data InboxItem =
    RequestFromPeer PeerName PeerRequest
  | ResponseFromPeer PeerName WaitingCallback PeerResponse
  | ClockTick Int

instance Show InboxItem where
  show (RequestFromPeer name req) = "RequestFromPeer " ++ show name ++ " " ++ show req
  show (ResponseFromPeer name _ resp) = "ResponseFromPeer " ++ show name ++ " _ " ++ show resp
  show (ClockTick i) = "ClockTick " ++ show i

data NodeSim = NodeSim {
  _nodeEnv :: ProtocolEnv
, _nodeState :: RaftState
, _nodeInbox :: Seq InboxItem
, _nodePendingResponses :: Seq (WaitingCallback)
}

data Network = Network {
  _nodes :: Map PeerName NodeSim
}

makeLenses ''NodeSim
makeLenses ''Network

makeNode :: Set PeerName -> PeerName -> NodeSim
makeNode allPeers self = NodeSim newnodeEnv newnodeState Seq.empty Seq.empty
  where
    newnodeEnv = (ProtocolEnv self (Set.difference allPeers $ Set.singleton self) :: ProtocolEnv)
    newnodeState = mkRaftState


applyState :: InboxItem -> ProtoStateMachine ()
applyState (ClockTick i) = do
  processTick () $ Tick $ fromIntegral i

applyState (RequestFromPeer sender req) = do
  processPeerRequestMessage req $ peerIdOfName sender
applyState (ResponseFromPeer _sender (WaitingCallback f) resp) = f resp

-- applyState other = error $ "applyState: " ++ show other

spec :: Spec
spec = do
  describe "startup" $ do
    it "removes leading and trailing whitespace" $ do
      () `shouldBe` ()
    it "Starts in Follower mode" $ do
      let simulation = forM [0..32] $ \i -> do
            simulateIteration i
            res <- get
            return (i, res)

      (states, _) <- State.runStateT (runStderrLoggingT simulation) network
      forM_ states $ \(i, network') -> do
        putStrLn $ "T: " ++ show i
        forM_ (Map.toList $ view nodes network') $ \(name, node) ->
          putStrLn $ unPeerName name ++ "\t" ++ show (view nodeState node)
      pending
  where
      allPeers = Set.fromList $ fmap PeerName ["a", "b", "c"]
      allNodes = Map.fromList $ fmap (\p -> (p , makeNode allPeers p)) $ Set.toList allPeers
      network = Network allNodes

simulateIteration :: (MonadLogger m, MonadState Network m) => Int -> m ()
simulateIteration n = do
    broadcastTick n
    go

  where
  go = do
    simulateStep

    quiescentp <- getQuiesecent
    if quiescentp
      then return ()
      else go

getQuiesecent :: (MonadLogger m, MonadState Network m) => m Bool
getQuiesecent = do
  st <- get
  let inboxes = toListOf (nodes . each . nodeInbox) st
  $(logDebugSH) ("Inboxes: ", inboxes)
  return $ all Seq.null $ inboxes

broadcastTick :: (MonadLogger m, MonadState Network m) => Int -> m ()
broadcastTick n = do
  (nodes . each . nodeInbox) %= (|> ClockTick n)

simulateStep :: (MonadLogger m, MonadState Network m) => m ()
simulateStep = do
  allNodes <- use nodes
  outputs <- forM (Map.toList allNodes) $ \(name, node) -> do
    $(logDebugSH) ("Node:", name, view nodeInbox node)
    let actions = traverse_ applyState $ view nodeInbox node
    let ((), s', toSend) = RWS.runRWS (runProto actions) (view nodeEnv node) (view nodeState node)
    nodes . ix name . nodeState .= s'
    nodes . ix name . nodeInbox .= Seq.empty
    return (name, toSend)

  forM_ outputs $ \(fromName, msgs) -> do
    forM_ msgs $ \msg -> do
      case msg of
        PeerRequest name m cb -> do
          $(logDebugSH) (fromName, name, "PeerRequest", m)
          nodes . ix name . nodeInbox %= (|> RequestFromPeer fromName m)
          nodes . ix fromName . nodePendingResponses %= (|> WaitingCallback cb)
        PeerReply peerId m -> do
          let name = nameOfPeerId peerId
          $(logDebugSH) (fromName, name, "PeerReply", m)
          cb <- fromMaybe (error "No waiting callback?") <$> preview (nodes . ix name . nodePendingResponses . each) <$> get
          nodes . ix name . nodeInbox %= (|> ResponseFromPeer fromName cb m)
          return ()
        Reply clientId m -> do
          $(logDebugSH) (fromName, clientId, "reply", m)
          error $ "something something client reply" ++ show (clientId, m)

  return ()

nameOfPeerId :: IdFor PeerResponse -> PeerName
nameOfPeerId (IdFor 0) = PeerName "a"
nameOfPeerId (IdFor 1) = PeerName "b"
nameOfPeerId (IdFor 2) = PeerName "c"
nameOfPeerId other = error $ "Unnown peer id: " ++ show other

peerIdOfName :: PeerName -> IdFor PeerResponse
peerIdOfName (PeerName "a") = (IdFor 0)
peerIdOfName (PeerName "b") = (IdFor 1)
peerIdOfName (PeerName "c") = (IdFor 2)
peerIdOfName other = error $ "Unnown peer name: " ++ show other
