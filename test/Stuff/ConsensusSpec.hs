{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Stuff.ConsensusSpec (
  tests
, prop_simulateLeaderElection
) where

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Control.Monad
import Data.Sequence (Seq, (|>), ViewL(..))
import qualified Data.Sequence as Seq
import Data.Foldable (traverse_, foldl')
import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Monad.Trans.State.Strict as State
-- import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..), get)
-- import           Control.Monad.Reader.Class (MonadReader(..), asks)
import Control.Monad.Logger
import qualified Debug.Trace as Trace
import Data.Ratio ((%))
import GHC.Stack

import Lens.Micro.Platform

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import Stuff.RaftModel
import Stuff.Proto
import Stuff.Types

newtype WaitingCallback = WaitingCallback (PeerResponse -> ProtoStateMachine ())

data InboxItem =
    RequestFromPeer PeerName PeerRequest
  | ResponseFromPeer PeerName WaitingCallback PeerResponse
  | ClockTick Time

instance Show InboxItem where
  show (RequestFromPeer name req) = "RequestFromPeer " ++ show name ++ " " ++ show req
  show (ResponseFromPeer name _ resp) = "ResponseFromPeer " ++ show name ++ " _ " ++ show resp
  show (ClockTick t) = "ClockTick " ++ show t

data NodeSim = NodeSim {
  _nodeEnv :: ProtocolEnv
, _nodeState :: !RaftState
, _nodeInbox :: Seq InboxItem
, _nodePendingResponses :: Seq (WaitingCallback)
}

data Network = Network {
  _nodes :: Map PeerName NodeSim
}

makeLenses ''NodeSim
makeLenses ''Network

allPeers :: Set PeerName
allPeers = Set.fromList $ fmap PeerName ["a", "b", "c"]

makeNode :: PeerName -> Time -> NodeSim
makeNode self elTimeout = NodeSim newnodeEnv newnodeState Seq.empty Seq.empty
  where
    newnodeEnv = (mkProtocolEnv self (Set.difference allPeers $ Set.singleton self) elTimeout (elTimeout / 3.0) :: ProtocolEnv)
    newnodeState = mkRaftState


applyState :: HasCallStack => InboxItem -> ProtoStateMachine ()
applyState (ClockTick t) = do
  processTick () $ Tick t

applyState (RequestFromPeer sender req) = do
  processPeerRequestMessage req $ peerIdOfName sender
applyState (ResponseFromPeer _sender (WaitingCallback f) resp) = f resp

-- applyState other = error $ "applyState: " ++ show other

{-
In order to ensure that we converge on an elected leader, we need to ensure
that at least one node is able to broadcast. We can do that via either:

 * Randomizing timeouts
 * Randomizing schedules

The former requires a Random instance to be carried in the model. The latter
requires that we generate a random schedule in the _test_ (eg: via hedgehog)
and iterate over that. Logically, the schedule is an acyclic graph, where
verteces are activations where a node can process it's inputs, and edges are
thefore the time taken to transmit a message between nodes.

As a simpler example though, we can represent it as a sequence, sort of, or
`Map Time ProcessId`, where ``. Messages
in this model are effectively delivered immediately, and will be processed at
the node's next activation.
-}

data ProcessId =
    Client Int
  | Clock
  | Node PeerName
  deriving (Show)

peerName :: Gen PeerName
peerName = Gen.element $ Set.toList allPeers

processId :: Gen ProcessId
processId = Gen.choice
  [ Node <$> peerName
  , Gen.constant Clock
  ]

ticksPerSecond :: Integer
ticksPerSecond = 1000

timestamp :: Integer -> Gen Integer
timestamp len = do
  num <- Gen.integral (Range.linear 0 (len * ticksPerSecond))
  return $ num

-- Technically, a Set of processid should be activated at each timestep.
-- But, at the moment, clocks are instantanious and global, wheras they should
-- be per process, really.

schedule :: Gen (Map Integer ProcessId)
schedule = Gen.map (Range.linear 0 1000) $ ((,) <$> timestamp 100 <*> processId)

{-
We know that _only_ leaders will emit AppendEntries events, so rather than
inspecting the node state directly, we can listen for `AppendEntries` messages
from nodes, and record the source against the term.
-}

prop_simulateLeaderElection :: Property
prop_simulateLeaderElection = property $ do
      ts <- forAll timeouts
      sched <- forAll schedule
      let network = Network (allNodes $ map (% ticksPerSecond) ts)
      states <- evalIO $ do
        putStrLn "---"
        putStrLn "Running:"
        Trace.traceShow ("Timeouts", ts) $ return ()
        Trace.traceShow ("Schedule", sched) $ return ()
        let simulation = forM (Map.toList sched) $ \(t, node) -> do
              msgs <- simulateIteration t node
              return msgs
        mconcat <$> fst <$> State.runStateT (runStderrLoggingT simulation) network

      let _ = states :: [(PeerName, ProcessorMessage)]

      test $ do
        -- We should use Map.unionWith Set.union here

        let allLeaders = leadersByTerm states
        Trace.traceShow ("Leaders by term", allLeaders) $ return ()

        footnoteShow $ ("Leaders by term", allLeaders)
        forM_ (Map.toList allLeaders) $ \(_term, leaders) -> do
          assert $ 1 >= Set.size leaders

      evalIO $ putStrLn $ "Okay!"
  where
      allNodes ts = Map.fromList $ fmap (\(p, t) -> (p , makeNode p t)) $ zip (Set.toList allPeers) ts
      leadersByTerm :: [(PeerName, ProcessorMessage)] -> Map Term (Set PeerName)
      leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ map leadersOf events
        -- let allLeaders = List.foldl' ... $
      leadersOf :: (PeerName, ProcessorMessage) -> Map Term (Set PeerName)
      leadersOf (sender, PeerRequest _ (AppendEntries aer) _) = Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
      leadersOf _ = Map.empty

      timeouts = Gen.list (Range.constant nnodes nnodes) $ ((+ 2500) <$> timestamp 1)
      nnodes = Set.size allPeers

simulateIteration :: (MonadLogger m, MonadState Network m) => Integer -> ProcessId -> m [(PeerName, ProcessorMessage)]
simulateIteration n Clock = do
    let t = n % ticksPerSecond
    $(logDebugSH) ("Clock", t)
    broadcastTick t
    return []
simulateIteration _ (Node name) = do
    simulateStep name

simulateIteration _ other = error $ "simulateIteration: " ++ show other

getQuiesecent :: (MonadLogger m, MonadState Network m) => PeerName -> m Bool
getQuiesecent name = do
  inboxes <- toListOf (nodes . ix name . nodeInbox) <$> get
  $(logDebugSH) ("Inbox", name, inboxes)
  return $ all Seq.null $ inboxes

broadcastTick :: (MonadLogger m, MonadState Network m) => Time -> m ()
broadcastTick n = do
  (nodes . each . nodeInbox) %= (|> ClockTick n)

simulateStep :: (HasCallStack, MonadLogger m, MonadState Network m) => PeerName -> m [(PeerName, ProcessorMessage)]
simulateStep thisNode = do
  node <- fromMaybe (error $ "No node: " ++ show thisNode) <$> preview (nodes . ix thisNode) <$> get
  $(logDebugSH) ("Run", thisNode, view nodeInbox node)
  Trace.traceShow ("Run", thisNode, view nodeInbox node) $ return ()
  let actions = traverse_ applyState $ view nodeInbox node
  let ((), s', toSend) = RWS.runRWS (runProto actions) (view nodeEnv node) (view nodeState node)
  nodes . ix thisNode . nodeState .= (s' `seq` s')
  nodes . ix thisNode . nodeInbox .= Seq.empty

  $(logDebugSH) ("Outputs from ", thisNode, toSend)

  forM_ toSend $ \msg -> do
    case msg of
      PeerRequest name m cb -> do
        $(logDebugSH) (unPeerName thisNode, "->", unPeerName name, "PeerRequest", m)
        nodes . ix name . nodeInbox %= (|> RequestFromPeer thisNode m)
        nodes . ix thisNode . nodePendingResponses %= (|> WaitingCallback cb)
      PeerReply peerId m -> do
        let name = nameOfPeerId peerId
        $(logDebugSH) (unPeerName name, "<-", unPeerName thisNode, "PeerReply", m)
        pending <- use (nodes . ix name . nodePendingResponses)
        case Seq.viewl pending of
          Seq.EmptyL -> error "No waiting callback?"
          cb :< rest -> do
            (nodes . ix name . nodePendingResponses)  .= rest
            nodes . ix name . nodeInbox %= (|> ResponseFromPeer thisNode cb m)
        return ()
      Reply clientId m -> do
        $(logDebugSH) (thisNode, clientId, "reply", m)
        error $ "something something client reply" ++ show (clientId, m)

  return $ map (\m -> (thisNode, m)) toSend

nameOfPeerId :: IdFor PeerResponse -> PeerName
nameOfPeerId (IdFor 0) = PeerName "a"
nameOfPeerId (IdFor 1) = PeerName "b"
nameOfPeerId (IdFor 2) = PeerName "c"
nameOfPeerId (IdFor 3) = PeerName "d"
nameOfPeerId (IdFor 4) = PeerName "e"
nameOfPeerId other = error $ "Unnown peer id: " ++ show other

peerIdOfName :: PeerName -> IdFor PeerResponse
peerIdOfName (PeerName "a") = (IdFor 0)
peerIdOfName (PeerName "b") = (IdFor 1)
peerIdOfName (PeerName "c") = (IdFor 2)
peerIdOfName (PeerName "d") = (IdFor 3)
peerIdOfName (PeerName "e") = (IdFor 4)
peerIdOfName other = error $ "Unnown peer name: " ++ show other


-- Driver bits


tests :: Group
tests = $$(discover)