{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Stuff.ConsensusSpec (
  tests
) where

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Control.Monad
import Data.Sequence (Seq, (|>), ViewL(..))
import qualified Data.Sequence as Seq
import qualified Data.List as List
import qualified Data.Bimap as Bimap
import Data.Bimap (Bimap)
import Data.Foldable (traverse_, foldl', toList)
import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Monad.Trans.State.Strict as State
-- import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..), get)
-- import           Control.Monad.Reader.Class (MonadReader(..), asks)
import qualified Data.Functor.Identity as Identity
import qualified Control.Monad.Logger as Logger
import Control.Monad.Logger
import Data.Ratio ((%))
import GHC.Stack
import Data.Hashable
import Text.Show.Pretty
import qualified Data.Text as Text
import Control.Applicative ((<|>))

import Lens.Micro.Platform

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import Stuff.RaftModel
import Stuff.Proto
import Stuff.Types

newtype WaitingCallback = WaitingCallback (PeerResponse -> ProtoStateMachine ())

type PeerMap = Bimap PeerName (IdFor PeerResponse)

instance Show WaitingCallback where
  show _ = "WaitingCallback"

data InboxItem =
    RequestFromPeer PeerName PeerRequest
  | ResponseFromPeer PeerName WaitingCallback PeerResponse
  | ClockTick Time
  | ClientRequest (IdFor ClientResponse) ClientRequest

instance Show InboxItem where
  show (RequestFromPeer name req) = "RequestFromPeer " ++ show name ++ " " ++ show req
  show (ResponseFromPeer name _ resp) = "ResponseFromPeer " ++ show name ++ " WaitingCallback " ++ show resp
  show (ClockTick t) = "ClockTick " ++ show t
  show (ClientRequest rid req) = "ClientRequest " ++ show rid ++ " " ++ show req

-- The IdFor fields for MessageOut should match the matching MessageIn
data MessageEvent =
    MessageIn (IdFor MessageEvent) PeerName InboxItem
  | MessageOut (IdFor MessageEvent) PeerName ProcessorMessage
  deriving (Show)

data NodeSim = NodeSim {
  _nodeEnv :: ProtocolEnv
, _nodeState :: !RaftState
, _nodeInbox :: Seq (IdFor MessageEvent, InboxItem)
, _nodePendingResponses :: Map PeerName (Seq WaitingCallback)
} deriving (Show)

data ClientSim = ClientSim {
  _lastSeenLeader :: Maybe PeerName
, _sentReqId :: Int
} deriving (Show)

data Network = Network {
  _nodes :: Map PeerName NodeSim
, _client :: ClientSim
, _idCounter :: Int
} deriving (Show)

makeLenses ''NodeSim
makeLenses ''ClientSim
makeLenses ''Network

makeNode :: PeerMap -> PeerName -> Time -> NodeSim
makeNode allPeers self elTimeout = NodeSim newnodeEnv mkRaftState Seq.empty Map.empty
  where
    newnodeEnv = (mkProtocolEnv self (Set.difference names $ Set.singleton self) elTimeout (elTimeout / 3.0) :: ProtocolEnv)
    names = Set.fromList $ Bimap.keys allPeers


applyState :: HasCallStack => Bimap PeerName (IdFor PeerResponse) -> InboxItem -> ProtoStateMachine ()
applyState _ (ClockTick t) = do
  processTick () $ Tick t

applyState allPeers (RequestFromPeer sender req) = do
  processPeerRequestMessage req $ peerIdOfName allPeers sender

applyState _ (ResponseFromPeer _sender (WaitingCallback f) resp) = f resp

applyState _ (ClientRequest rid cmd) = do
  processClientReqRespMessage cmd rid

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
`Map Time ProcessActivation`, where ``. Messages
in this model are effectively delivered immediately, and will be processed at
the node's next activation.
-}

data ProcessActivation =
    Client ClientRequest
  | Clock PeerName
  | Node PeerName
  deriving (Show)

peerName :: PeerMap -> Gen PeerName
peerName allPeers = Gen.element $ Bimap.keys allPeers

clientCommand :: Gen ClientRequest
clientCommand = Gen.choice
  [ Gen.constant Bing
  , Gen.constant Ping
  ]

serverIds :: PeerMap -> Gen ProcessActivation
serverIds allPeers = Gen.choice
  [ Node <$> peerName allPeers
  , Clock <$> peerName allPeers
  ]

clientIds :: Gen ProcessActivation
clientIds = Client <$> clientCommand


timestamp :: Integer -> Gen Integer
timestamp len = do
  num <- Gen.integral (Range.linear 0 (len * ticksPerSecond))
  return $ num

-- Technically, a Set of processid should be activated at each timestep.
-- But, at the moment, clocks are instantanious and global, wheras they should
-- be per process, really.

-- The number of "seconds" that we run for.
simLength :: Integral a => a
simLength = 100
ticksPerSecond :: Integer
ticksPerSecond = 1000
eventsPerSecond :: Int
eventsPerSecond = 1

aPeerName :: Gen PeerName
aPeerName = PeerName <$> Gen.string (Range.linear 1 4) Gen.lower

peers :: Int -> Gen (PeerMap)
peers npeers = Bimap.fromList <$> (zip <$> names <*> (map (IdFor . hash) <$> names))
  where
    possibleNames = pure $ map (PeerName . show) ([0..] :: [Integer])
    names = take <$> Gen.int (Range.singleton npeers) <*> possibleNames

schedule :: Gen ProcessActivation -> Gen (Map Integer ProcessActivation)
schedule pids = Gen.map (Range.linear 0 (simLength * eventsPerSecond)) $ ((,) <$> timestamp simLength <*> pids)

{-
We know that _only_ leaders will emit AppendEntries events, so rather than
inspecting the node state directly, we can listen for `AppendEntries` messages
from nodes, and record the source against the term.
-}
timeouts :: PeerMap -> Gen [Integer]
timeouts allPeers = Gen.list (Range.constant nnodes nnodes) $ ((+ 2500) <$> timestamp 1)
  where
  nnodes = Bimap.size allPeers

runSimulation :: PeerMap -> [Integer] -> Map Integer ProcessActivation -> String -> PropertyT IO [MessageEvent]
runSimulation allPeers ts sched fname = do
      footnoteShow $ "Logging to: " ++ fname

      let network = Network allNodes (ClientSim Nothing 0) 0
      evalIO $ do
        runFileLoggingT fname $ do
          $(logDebug) $ Text.pack $ ppShow ("allPeers", allPeers)
          $(logDebug) $ Text.pack $ ppShow ("timeouts", ts)
          $(logDebug) $ Text.pack $ ppShow ("sched", sched)
        let simulation = forM (Map.toList sched) $ \(t, node) -> do
              msgs <- simulateIteration allPeers t node
              return msgs
        (msgs, network') <- State.runStateT (runFileLoggingT fname simulation) network
        runFileLoggingT fname $ do
          $(logDebug) $ Text.pack $ ppShow ("Network", network')
        return $ mconcat $ msgs

  where
    allNodes = Map.fromList $ fmap (\(p, t) -> (p, makeNode allPeers p t)) $ zip (Bimap.keys allPeers) (map (% ticksPerSecond) ts)

prop_leaderElectionOnlyElectsOneLeaderPerTerm_2 :: HasCallStack => Property
prop_leaderElectionOnlyElectsOneLeaderPerTerm_2 = leaderElectionOnlyElectsOneLeaderPerTerm 2
prop_leaderElectionOnlyElectsOneLeaderPerTerm_3 :: HasCallStack => Property
prop_leaderElectionOnlyElectsOneLeaderPerTerm_3 = leaderElectionOnlyElectsOneLeaderPerTerm 3
prop_leaderElectionOnlyElectsOneLeaderPerTerm_5 :: HasCallStack => Property
prop_leaderElectionOnlyElectsOneLeaderPerTerm_5 = leaderElectionOnlyElectsOneLeaderPerTerm 5
prop_leaderElectionOnlyElectsOneLeaderPerTerm_7 :: HasCallStack => Property
prop_leaderElectionOnlyElectsOneLeaderPerTerm_7 = leaderElectionOnlyElectsOneLeaderPerTerm 7


leaderElectionOnlyElectsOneLeaderPerTerm :: HasCallStack => Int -> Property
leaderElectionOnlyElectsOneLeaderPerTerm n = property $ do
      allPeers <- forAll $ peers n
      ts <- forAll $ timeouts allPeers
      sched <- forAll $ schedule $ serverIds allPeers
      let h = hash $ show (allPeers, ts, sched)
      let fname = "/tmp/prop_leaderElectionOnlyElectsOneLeaderPerTerm_" ++ show h

      messages <- runSimulation allPeers ts sched fname

      let allLeaders = leadersByTerm messages

      test $ do
        footnoteShow $ ("Leaders by term", allLeaders)
        forM_ (Map.toList allLeaders) $ \(_term, leaders) -> do
          assert $ 1 >= Set.size leaders

  where
      leadersByTerm :: [MessageEvent] -> Map Term (Set PeerName)
      leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ map leadersOf events
        -- let allLeaders = List.foldl' ... $
      leadersOf :: MessageEvent -> Map Term (Set PeerName)
      leadersOf (MessageOut _mid sender (PeerRequest _ (AppendEntries aer) _)) =
          Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
      leadersOf _ = Map.empty

prop_bongsAreMonotonic_1 :: HasCallStack => Property
prop_bongsAreMonotonic_1 = bongsAreMonotonic 1
prop_bongsAreMonotonic_2 :: HasCallStack => Property
prop_bongsAreMonotonic_2 = bongsAreMonotonic 2
prop_bongsAreMonotonic_3 :: HasCallStack => Property
prop_bongsAreMonotonic_3 = bongsAreMonotonic 3
prop_bongsAreMonotonic_5 :: HasCallStack => Property
prop_bongsAreMonotonic_5 = bongsAreMonotonic 5


bongsAreMonotonic :: HasCallStack => Int -> Property
bongsAreMonotonic n = property $ do
      allPeers <- forAll $ peers n
      ts <- forAll $ timeouts allPeers
      sched <- forAll $ schedule $ serverIds allPeers <|> clientIds
      let h = hash $ show (allPeers, ts, sched)
      let fname = "/tmp/prop_bongsAreMonotonic" ++ show h

      messages <- runSimulation allPeers ts sched fname
      let respValues = foldr findResponse [] messages
      test $ do
        runFileLoggingT fname $ do
          -- $(logDebug) $ Text.pack $ ppShow ("messages", messages)
          $(logDebug) $ Text.pack $ ppShow ("responses", respValues)
          $(logDebug) $ Text.pack $ ppShow ("okay?", respValues == List.sort respValues)
          -- footnoteShow messages
        respValues === List.sort respValues

  where
    findResponse (MessageOut _ _ (Reply _ (Right val))) r = val : r
    findResponse _ r = r

prop_eventuallyElectsLeader_2 :: HasCallStack => Property
prop_eventuallyElectsLeader_2 = eventuallyElectsLeader 2
prop_eventuallyElectsLeader_3 :: HasCallStack => Property
prop_eventuallyElectsLeader_3 = eventuallyElectsLeader 3
prop_eventuallyElectsLeader_5 :: HasCallStack => Property
prop_eventuallyElectsLeader_5 = eventuallyElectsLeader 5
prop_eventuallyElectsLeader_7 :: HasCallStack => Property
prop_eventuallyElectsLeader_7 = eventuallyElectsLeader 7


eventuallyElectsLeader :: HasCallStack => Int -> Property
eventuallyElectsLeader n = property $ do
      allPeers <- forAll $ peers n
      ts <- forAll $ timeouts allPeers
      sched <- forAll $ schedule $ serverIds allPeers
      let h = hash $ show (allPeers, ts, sched)
      let fname = "/tmp/prop_eventuallyElectsLeader_" ++ show h

      -- At this point, we want to be able to have a _cyclic_ schedule per
      -- node, so that they're not all running in lockstep. However, ClockTick
      -- currently specify absolute time, so that'll need to be taken into
      -- account. 1) Generate a finite sub-schedule per entity 2) Create a
      -- lazy cyclic list of same 3) Merge into priority queue lazily

      -- Per process: [(DelayTime, Event)]
      -- Event = CheckInbox | Tick

      messages <- simulate allPeers ts sched fname

      let allLeaders = leadersByTerm messages

      test $ do
        footnoteShow $ ("Leaders by term", allLeaders)
        forM_ (Map.toList allLeaders) $ \(_term, leaders) -> do
          assert $ 1 >= Set.size leaders

  where
      leadersByTerm :: [MessageEvent] -> Map Term (Set PeerName)
      leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ map leadersOf events
        -- let allLeaders = List.foldl' ... $
      leadersOf :: MessageEvent -> Map Term (Set PeerName)
      leadersOf (MessageOut _mid sender (PeerRequest _ (AppendEntries aer) _)) =
          Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
      leadersOf _ = Map.empty

      simulate :: PeerMap -> [Integer] -> Map Integer ProcessActivation -> String -> PropertyT IO [MessageEvent]
      simulate allPeers ts sched fname = do
            footnoteShow $ "Logging to: " ++ fname

            let network = Network (allNodes allPeers ts) (ClientSim Nothing 0) 0
            evalIO $ do
              runFileLoggingT fname $ do
                $(logDebug) $ Text.pack $ ppShow ("allPeers", allPeers)
                $(logDebug) $ Text.pack $ ppShow ("timeouts", ts)
                $(logDebug) $ Text.pack $ ppShow ("sched", sched)
              let simulation = forM (Map.toList sched) $ \(t, node) -> do
                    msgs <- simulateIteration allPeers t node
                    return msgs
              (msgs, network') <- State.runStateT (runFileLoggingT fname simulation) network
              runFileLoggingT fname $ do
                $(logDebug) $ Text.pack $ ppShow ("Network", network')
              return $ mconcat $ msgs
      allNodes allPeers ts = Map.fromList $ fmap (\(p, t) -> (p, makeNode allPeers p t)) $ zip (Bimap.keys allPeers) (map (% ticksPerSecond) ts)



-- mechanics

nextId :: MonadState Network m => m (IdFor a)
nextId = IdFor <$> (idCounter <<%= succ)

simulateIteration :: (HasCallStack, MonadLogger m, MonadState Network m)
                  => PeerMap
                  -> Integer
                  -> ProcessActivation
                  -> m [MessageEvent]
simulateIteration _ n (Clock name) = do
    let t = n % ticksPerSecond
    mid <- nextId
    $(logDebugSH) ("Clock", name, t)
    (nodes . ix name . nodeInbox) %= (|> (mid, ClockTick t))
    return []

simulateIteration allPeers _ (Node name) = do
    simulateStep allPeers name

simulateIteration allPeers n (Client cmd) = do
    let t = n % ticksPerSecond
    rid <- IdFor <$> (client . sentReqId  <<%= succ)
    mid <- nextId
    use client >>= \st -> $(logDebugSH) ("simulateIteration", Client cmd, rid, st)
    lastSeen <- use (client . lastSeenLeader)
    let peer = fromMaybe (Bimap.keys allPeers !!  (fromInteger n `mod` Bimap.size allPeers)) lastSeen
    $(logDebugSH) ("Client", peer, lastSeen, rid, t)
    (nodes . ix peer . nodeInbox) %= (|> (mid, ClientRequest rid cmd))
    return []

getQuiesecent :: (MonadLogger m, MonadState Network m) => PeerName -> m Bool
getQuiesecent name = do
  inboxes <- toListOf (nodes . ix name . nodeInbox) <$> get
  $(logDebugSH) ("Inbox", name, inboxes)
  return $ all Seq.null $ inboxes


simulateStep :: (HasCallStack, MonadLogger m, MonadState Network m) => PeerMap -> PeerName -> m [MessageEvent]
simulateStep allPeers thisNode = do
  node <- fromMaybe (error $ "No node: " ++ show thisNode) <$> preview (nodes . ix thisNode) <$> get
  let inbox = view nodeInbox node
  nodes . ix thisNode . nodeInbox .= Seq.empty

  $(logDebugSH) ("Run", thisNode, inbox)
  let actions = flip traverse_ inbox $ \(mid, it) -> do
                  $(logDebugSH) ("apply", thisNode, mid, it)
                  applyState allPeers it

  let (((), s', toSend), logs) = Identity.runIdentity $
                                 Logger.runWriterLoggingT $
                                 RWS.runRWST (runProto $ actions) (view nodeEnv node) (view nodeState node)
  forM_ logs $ \(loc, src, lvl, logmsg) -> Logger.monadLoggerLog loc src lvl logmsg

  nodes . ix thisNode . nodeState .= (s' `seq` s')

  $(logDebugSH) ("Outputs from ", thisNode, toSend)

  outs <- forM toSend $ \msg -> do
    mid <- nextId
    case msg of
      PeerRequest dst m cb -> sendPeerRequest mid dst m cb
      PeerReply peerId m -> sendPeerReply mid peerId m
      Reply clientId m -> sendClientReply clientId m
    return $ MessageOut mid thisNode msg

  let ins = map (\(mid, msg) -> MessageIn mid thisNode msg) $ toList inbox
  return $ ins ++ outs
  where


  sendPeerRequest mid dst m cb= do
        $(logDebugSH) (unPeerName thisNode, "->", unPeerName dst, "PeerRequest", m)
        nodes . ix dst . nodeInbox %= (|> (mid, RequestFromPeer thisNode m))
        nodes . ix thisNode . nodePendingResponses %= (Map.insertWith (flip mappend) dst $ Seq.singleton $ WaitingCallback cb)
        do
          p <-  use $ nodes . ix thisNode . nodePendingResponses
          $(logDebugSH) (unPeerName thisNode, "post sendPeerRequest", "num pending callbacks", fmap Seq.length p)

  sendPeerReply mid peerId m = do
        let dst = nameOfPeerId allPeers peerId
        $(logDebugSH) (unPeerName dst, "<-", unPeerName thisNode, "PeerReply", m)

        do
          p <-  use $ nodes . ix dst . nodePendingResponses
          $(logDebugSH) (unPeerName dst, "pre sendPeerReply", "num pending callbacks", fmap Seq.length p)

        pending <- use (nodes . ix dst . nodePendingResponses . ix thisNode)
        case Seq.viewl pending of
          Seq.EmptyL -> error "No waiting callback?"
          cb :< rest -> do
            (nodes . ix dst . nodePendingResponses . ix thisNode)  .= rest
            nodes . ix dst . nodeInbox %= (|> (mid, ResponseFromPeer thisNode cb m))
        do
          p <-  use $ nodes . ix dst . nodePendingResponses
          $(logDebugSH) (unPeerName dst, "post sendPeerReply", "num pending callbacks", fmap Seq.length p)
        return ()

  sendClientReply clientId m@(Left (NotLeader leaderp)) = do
        st <- use client
        $(logDebugSH) (thisNode, clientId, "pre  sendClientReply", m, st)
        client . lastSeenLeader .= leaderp
        st' <- use client
        $(logDebugSH) (thisNode, clientId, "post sendClientReply", m, st')
  sendClientReply clientId m@(Right (Bong _)) = do
        st <- use client
        $(logDebugSH) (thisNode, clientId, "sendClientReply", m, st)

nameOfPeerId :: PeerMap -> IdFor PeerResponse -> PeerName
nameOfPeerId m name = m Bimap.!> name

peerIdOfName :: PeerMap -> PeerName -> IdFor PeerResponse
peerIdOfName m name = m Bimap.! name

-- Driver bits


tests :: Group
tests = $$(discover)
