{-# LANGUAGE FlexibleContexts #-}
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
import           Control.Monad.State.Class (MonadState(..), modify, get)
-- import           Control.Monad.Reader.Class (MonadReader(..), asks)

import Stuff.RaftModel
import Stuff.Proto

data InboxItem =
    RequestFromPeer PeerRequest
  | ResponseFromPeer PeerResponse
  deriving (Show)

data NodeSim = NodeSim {
  nodeEnv :: ProtocolEnv
, nodeState :: RaftState
, nodeInbox :: Seq InboxItem
} deriving (Show)

data Network = Network {
  nodes :: Map PeerName NodeSim
}

makeNode :: Set PeerName -> PeerName -> NodeSim
makeNode allPeers self = Network $ NodeSim newnodeEnv newnodeState Seq.empty
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
      forM_ (Map.toList _allNodes') $ print
      pending

simulateIteration :: MonadState (Map PeerName NodeSim) m => m ()
simulateIteration = do
  simulateStep

  st <- get
  when $ not $ isQuiesecent st $ simulateIteration


isQuiesecent :: Network -> Bool
isQuiesecent  = error "isQuiesecent"

simulateStep :: MonadState Network m => m ()
simulateStep = do
  allNodes <- nodes <$> get
  outputs <- forM (Map.toList allNodes) $ \(name, node) -> do
    -- sequence?
    let actions = traverse_ applyState $ nodeInbox node
    let ((), s', toSend) = RWS.runRWS (runProto actions) (nodeEnv node) (nodeState node)
    let _ = toSend :: [ProcessorMessage]
    modify $ \n -> n { nodes = Map.insert name (node { nodeState = s' }) $ nodes n }
    return (name, toSend)

  forM_ outputs $ \(_fromName, msgs) -> do
    forM_ msgs $ \msg -> do
      case msg of
        PeerRequest name m _cb -> do
          modify $ Map.adjust (\node -> node { nodeInbox = nodeInbox node |> RequestFromPeer m}) name
          error "Do someething with cb"
        PeerReply peerId m -> do
          let name = nameOfPeerId peerId
          modify $ Map.adjust (\node -> node { nodeInbox = nodeInbox node |> ResponseFromPeer m}) name
        Reply clientId m -> do
          error $ "something something client reply" ++ show (clientId, m)

  return ()

nameOfPeerId :: IdFor PeerResponse -> PeerName
nameOfPeerId (IdFor 0) = PeerName "9990"
nameOfPeerId (IdFor 1) = PeerName "9991"
nameOfPeerId (IdFor 2) = PeerName "9992"
nameOfPeerId other = error $ "Unnown peer id: " ++ show other
