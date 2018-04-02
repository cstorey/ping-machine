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
import Data.Sequence (Seq, (|>))
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

instance Show InboxItem where
  show (RequestFromPeer name req) = "RequestFromPeer " ++ show name ++ " " ++ show req
  show (ResponseFromPeer name _ resp) = "ResponseFromPeer " ++ show name ++ " _ " ++ show resp

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

allPeers :: Set PeerName
allPeers = Set.fromList $ fmap PeerName ["a", "b", "c", "d", "e"]

makeNode :: PeerName -> Time -> NodeSim
makeNode self elTimeout = NodeSim newnodeEnv newnodeState Seq.empty Seq.empty
  where
    newnodeEnv = (ProtocolEnv self (Set.difference allPeers $ Set.singleton self) elTimeout (elTimeout / 3.0) :: ProtocolEnv)
    newnodeState = mkRaftState

applyState :: Integer -> InboxItem -> ProtoStateMachine ()
applyState n (RequestFromPeer sender req) = do
  let t = n % ticksPerSecond
  processTick () $ Tick t
  processPeerRequestMessage req $ peerIdOfName sender
applyState _ (ResponseFromPeer _sender (WaitingCallback f) resp) = f resp

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

-- Arguably, clock should tick at a given node.
-- Clock tick means that it just checks for timeouts.
data ProcessId =
    Client Int
  | Node PeerName
  deriving (Show)

peerName :: Gen PeerName
peerName = Gen.element $ Set.toList allPeers

processId :: Gen ProcessId
processId = Gen.choice
  [ Node <$> peerName ]

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

leadersOf :: Network -> Map Term (Set PeerName)
leadersOf net = leaders
  where
    nodeList :: [(PeerName, NodeSim)]
    nodeList = Map.toList $ view nodes net
    leaders :: Map Term (Set PeerName)
    leaders = Map.fromList $ do
            (name, state') <- nodeList
            let st = view nodeState state'
            let term = currentTerm st
            case currentRole st of
              Leader _ -> [(term, Set.singleton name)]
              _ -> []


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
        Trace.traceShow ("Schedule", sched) $ return ()
        let simulation = forM (Map.toList sched) $ \(t, node) -> do
              simulateIteration t node
              res <- get
              return (t, res)
        fst <$> State.runStateT (runStderrLoggingT simulation) network

      test $ do
        -- We should use Map.unionWith Set.union here

        let allLeaders = leadersByTerm states
        Trace.traceShow ("Leaders by term", allLeaders) $ return ()

        footnoteShow $ ("Leaders by term", allLeaders)
        forM_ (Map.toList allLeaders) $ \(_, leaders) -> do
          assert $ 1 >= Set.size leaders

      evalIO $ putStrLn $ "Okay!"
  where
      allNodes ts = Map.fromList $ fmap (\(p, t) -> (p , makeNode p t)) $ zip (Set.toList allPeers) ts
      leadersByTerm :: [(a, Network)] -> Map Term (Set PeerName)
      leadersByTerm states = foldl' (Map.unionWith Set.union) Map.empty $ map (leadersOf . snd) states
        -- let allLeaders = List.foldl' ... $
      timeouts = Gen.list (Range.constant 3 3) $ ((+ 2500) <$> timestamp 1)

simulateIteration :: (MonadLogger m, MonadState Network m) => Integer -> ProcessId -> m ()
simulateIteration t (Node name) = do
    go
  where
  go = do
    quiescentp <- getQuiesecent name
    if quiescentp
    then return ()
    else simulateStep t name >> go

simulateIteration _ other = error $ "simulateIteration: " ++ show other

getQuiesecent :: (MonadLogger m, MonadState Network m) => PeerName -> m Bool
getQuiesecent name = do
  inboxes <- toListOf (nodes . ix name . nodeInbox) <$> get
  $(logDebugSH) ("Inbox ", name, inboxes)
  return $ all Seq.null $ inboxes

simulateStep :: (MonadLogger m, MonadState Network m) => Integer -> PeerName -> m ()
simulateStep t thisNode = do
  node <- fromMaybe (error $ "No node: " ++ show thisNode) <$> preview (nodes . ix thisNode) <$> get
  $(logDebugSH) ("Node:", thisNode, view nodeInbox node)
  let actions = traverse_ (applyState t) $ view nodeInbox node
  let ((), s', toSend) = RWS.runRWS (runProto actions) (view nodeEnv node) (view nodeState node)
  nodes . ix thisNode . nodeState .= s'
  nodes . ix thisNode . nodeInbox .= Seq.empty

  forM_ toSend $ \msg -> do
    case msg of
      PeerRequest name m cb -> do
        $(logDebugSH) (thisNode, name, "PeerRequest", m)
        nodes . ix name . nodeInbox %= (|> RequestFromPeer thisNode m)
        nodes . ix thisNode . nodePendingResponses %= (|> WaitingCallback cb)
      PeerReply peerId m -> do
        let name = nameOfPeerId peerId
        $(logDebugSH) (thisNode, name, "PeerReply", m)
        cb <- fromMaybe (error "No waiting callback?") <$> preview (nodes . ix name . nodePendingResponses . each) <$> get
        nodes . ix name . nodeInbox %= (|> ResponseFromPeer thisNode cb m)
        return ()
      Reply clientId m -> do
        $(logDebugSH) (thisNode, clientId, "reply", m)
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


-- Driver bits


tests :: Group
tests = $$(discover)