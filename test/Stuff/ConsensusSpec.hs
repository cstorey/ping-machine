{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Stuff.ConsensusSpec (spec) where

import Test.Hspec

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Monad
import Data.Sequence (Seq, (|>))
import qualified Data.Sequence as Seq
import Data.Foldable (traverse_)
import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Monad.Trans.State.Strict as State
-- import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..), get)
-- import           Control.Monad.Reader.Class (MonadReader(..), asks)

import Lens.Micro.Platform

import Stuff.RaftModel
import Stuff.Proto

data InboxItem =
    RequestFromPeer PeerRequest
  | ResponseFromPeer PeerResponse
  deriving (Show)

data NodeSim = NodeSim {
  _nodeEnv :: ProtocolEnv
, _nodeState :: RaftState
, _nodeInbox :: Seq InboxItem
} deriving (Show)

makeLenses ''NodeSim

makeNode :: Set PeerName -> PeerName -> NodeSim
makeNode allPeers self = NodeSim newnodeEnv newnodeState Seq.empty
  where
    newnodeEnv = (ProtocolEnv self (Set.difference allPeers $ Set.singleton self) :: ProtocolEnv)
    newnodeState = mkRaftState


applyState :: InboxItem -> ProtoStateMachine ()
applyState = error "applyState"

spec :: Spec
spec = do
  describe "startup" $ do
    it "removes leading and trailing whitespace" $ do
      () `shouldBe` ()
    it "Starts in Follower mode" $ do
      let allPeers = Set.fromList $ fmap PeerName ["a", "b", "c"]
      let allNodes = Map.fromList $ fmap (\p -> (p , makeNode allPeers p)) $ Set.toList allPeers
      
      let ((), _allNodes') = State.runState simulateStep allNodes
      print _allNodes'
      pending

simulateStep :: MonadState (Map PeerName NodeSim) m => m ()
simulateStep = do
  allNodes <- get
  outputs <- forM (Map.toList allNodes) $ \(name, node) -> do
    -- sequence?
    let actions = traverse_ applyState $ view nodeInbox node
    let ((), s', toSend) = RWS.runRWS (runProto actions) (view nodeEnv node) (view nodeState node)
    let _ = toSend :: [ProcessorMessage]
    -- modify $ Map.insert name $ node { nodeState = s' }
    ix name . nodeState .= s'
    return (name, toSend)

  forM_ outputs $ \(_fromName, msgs) -> do
    forM_ msgs $ \msg -> do
      case msg of
        PeerRequest name m _cb -> do
          -- modify $ Map.adjust (\node -> node { nodeInbox = nodeInbox node |> RequestFromPeer m}) name
          (ix name . nodeInbox) %= (|> RequestFromPeer m)
          error "Do someething with cb"
        PeerReply peerId m -> do
          let name = nameOfPeerId peerId
          -- modify $ Map.adjust (\node -> node { nodeInbox = nodeInbox node |> ResponseFromPeer m}) name
          (ix name . nodeInbox) %= (|> ResponseFromPeer m)
          return ()
        Reply clientId m -> do
          error $ "something something client reply" ++ show (clientId, m)

  return ()

nameOfPeerId :: IdFor PeerResponse -> PeerName
nameOfPeerId (IdFor 0) = PeerName "9990"
nameOfPeerId (IdFor 1) = PeerName "9991"
nameOfPeerId (IdFor 2) = PeerName "9992"
nameOfPeerId other = error $ "Unnown peer id: " ++ show other
