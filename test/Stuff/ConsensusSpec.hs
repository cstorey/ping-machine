{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Stuff.ConsensusSpec (main) where

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
import           System.IO (BufferMode(..), hSetBuffering, stdout, stderr)
import           System.Exit (exitFailure)

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
  | ClockTick Double

instance Show InboxItem where
  show (RequestFromPeer name req) = "RequestFromPeer " ++ show name ++ " " ++ show req
  show (ResponseFromPeer name _ resp) = "ResponseFromPeer " ++ show name ++ " _ " ++ show resp
  show (ClockTick t) = "ClockTick " ++ show t

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
allPeers = Set.fromList $ fmap PeerName ["a", "b", "c"]

makeNode :: PeerName -> NodeSim
makeNode self = NodeSim newnodeEnv newnodeState Seq.empty Seq.empty
  where
    newnodeEnv = (ProtocolEnv self (Set.difference allPeers $ Set.singleton self) :: ProtocolEnv)
    newnodeState = mkRaftState


applyState :: InboxItem -> ProtoStateMachine ()
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

schedule :: Gen (Map Double ProcessId)
schedule = Gen.map (Range.linear 10 500) $ ((,) <$> Gen.double (Range.linearFrac 0 100) <*> processId)

leadersOf :: Network -> Set PeerName
leadersOf net =
  Set.fromList $
    fmap fst $
    filter (\(_, st) -> case currentRole $ view nodeState st of Leader _ -> True; _ -> False) $
    Map.toList $ view nodes net

prop_simulateLeaderElection :: Property
prop_simulateLeaderElection = property $ do
      sched <- forAll schedule
      states <- evalIO $ do
        let simulation = forM (Map.toList sched) $ \(t, node) -> do
              simulateIteration t node
              res <- get
              return (t, res)

        fst <$> State.runStateT (runStderrLoggingT simulation) network
      test $ do
        forM_ states $ \(t, network') -> do
          -- putStrLn $ "T: " ++ show t
          -- forM_ (Map.toList $ view nodes network') $ \(name, node) ->
            -- putStrLn $ unPeerName name ++ "\t" ++ show (view nodeState node)
          footnoteShow $ ("Leaders", t, leadersOf network')
          assert $ 1 >= Set.size (leadersOf network')
  where
      allNodes = Map.fromList $ fmap (\p -> (p , makeNode p)) $ Set.toList allPeers
      network = Network allNodes

simulateIteration :: (MonadLogger m, MonadState Network m) => Double -> ProcessId -> m ()
simulateIteration n Clock = do
    broadcastTick n
simulateIteration _ (Node name) = go
  where
  go = do
    quiescentp <- getQuiesecent name
    if quiescentp
    then return ()
    else simulateStep name >> go

simulateIteration _ other = error $ "simulateIteration: " ++ show other

getQuiesecent :: (MonadLogger m, MonadState Network m) => PeerName -> m Bool
getQuiesecent name = do
  inboxes <- toListOf (nodes . ix name . nodeInbox) <$> get
  $(logDebugSH) ("Inboxes: ", inboxes)
  return $ all Seq.null $ inboxes

broadcastTick :: (MonadLogger m, MonadState Network m) => Double -> m ()
broadcastTick n = do
  (nodes . each . nodeInbox) %= (|> ClockTick n)

simulateStep :: (MonadLogger m, MonadState Network m) => PeerName -> m ()
simulateStep thisNode = do
  node <- fromMaybe (error $ "No node: " ++ show thisNode) <$> preview (nodes . ix thisNode) <$> get
  $(logDebugSH) ("Node:", thisNode, view nodeInbox node)
  let actions = traverse_ applyState $ view nodeInbox node
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

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  res <- checkParallel $$(discover)
  unless res $ exitFailure
