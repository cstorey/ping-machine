{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Stuff.ConsensusSpec
( tests
, spec
) where

import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Control.Monad
import Data.Sequence (Seq, (|>), ViewL(..))
import qualified Data.Sequence as Seq
import qualified Data.List as List
import qualified Data.List.Ordered as List
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

import Debug.Trace as Trace()

import Lens.Micro.Platform

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           HaskellWorks.Hspec.Hedgehog
import           Test.Hspec

import Stuff.RaftModel
import Stuff.Proto
import Stuff.Models
import Stuff.Types

newtype WaitingCallback st req resp = WaitingCallback (PeerResponse -> ProtoStateMachine st req resp ())

type PeerMap = Bimap PeerName (IdFor PeerResponse)

instance Show (WaitingCallback st req resp) where
  show _ = "WaitingCallback"

data InboxItem st req resp =
    RequestFromPeer PeerName (PeerRequest req)
  | ResponseFromPeer PeerName (WaitingCallback st req resp) PeerResponse
  | ClockTick Time
  | ClientRequest (IdFor (ClientResponse resp)) req

instance (Show req) => Show (InboxItem st req resp) where
  show (RequestFromPeer name req) = "RequestFromPeer " ++ show name ++ " " ++ show req
  show (ResponseFromPeer name _ resp) = "ResponseFromPeer " ++ show name ++ " WaitingCallback " ++ show resp
  show (ClockTick t) = "ClockTick " ++ show t
  show (ClientRequest rid req) = "ClientRequest " ++ show rid ++ " " ++ show req

-- The IdFor fields for MessageOut should match the matching MessageIn
data MessageEvent st req resp =
    MessageIn (IdFor (MessageEvent st req resp)) PeerName (InboxItem st req resp)
  | MessageOut (IdFor (MessageEvent st req resp)) PeerName (ProcessorMessage st req resp)
  deriving (Show)

data NodeSim st req resp = NodeSim {
  _nodeEnv :: ProtocolEnv st req resp
, _nodeState :: !(RaftState req resp)
, _nodeInbox :: Seq (IdFor (MessageEvent st req resp), (InboxItem st req resp))
, _nodePendingResponses :: Map PeerName (Seq (WaitingCallback st req resp))
} deriving (Show)

data ClientSim req = ClientSim {
  _lastSeenLeader :: Maybe PeerName
, _sentReqId :: Int
, _newCommand :: req
} deriving (Show)

data Network st req resp = Network {
  _nodes :: Map PeerName (NodeSim st req resp)
, _client :: ClientSim req
, _idCounter :: Int
} deriving (Show)

makeLenses ''NodeSim
makeLenses ''ClientSim
makeLenses ''Network

makeNode :: PeerMap -> PeerName -> Time -> ModelFun st req resp -> st -> NodeSim st req resp
makeNode allPeers self elTimeout modelFn initSt = NodeSim newnodeEnv mkRaftState Seq.empty Map.empty
  where
    newnodeEnv = mkProtocolEnv self (Set.difference names $ Set.singleton self) elTimeout (elTimeout / 3.0) initSt modelFn
    names = Set.fromList $ Bimap.keys allPeers


applyState :: (HasCallStack, Show req, Show resp) => Bimap PeerName (IdFor PeerResponse) -> InboxItem st req resp -> ProtoStateMachine st req resp ()
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

--

At this point, we want to be able to have a _cyclic_ schedule per node, so
that they're not all running in lockstep. However, ClockTick currently specify
absolute time, so that'll need to be taken into account. 1) Generate a finite
sub-schedule per entity 2) Create a lazy cyclic list of same 3) Merge into
priority queue lazily

Per process: [(DelayTime, Event)]
Event = CheckInbox | Tick


-}

data ProcessId =
    Client
  | Node PeerName
  deriving (Show, Eq, Ord)

data ProcessEvent =
    Clock
  | Inbox
  deriving (Show, Eq, Ord)

type ProcessSchedule = [(Integer, ProcessEvent)]
newtype SystemSchedule = SystemSchedule (Map ProcessId [(Integer, ProcessEvent)])
  deriving (Show)

nodeProcs :: Bimap PeerName b -> [ProcessId]
nodeProcs allPeers = Node <$> Bimap.keys allPeers
clientProcs :: [ProcessId]
clientProcs = [Client]

peerName :: PeerMap -> Gen PeerName
peerName allPeers = Gen.element $ Bimap.keys allPeers

clientCommand :: Gen BingBongReq
clientCommand = Gen.choice
  [ Gen.constant Bing
  , Gen.constant Ping
  ]

serverIds :: PeerMap -> Gen ProcessId
serverIds allPeers = Node <$> peerName allPeers

clientIds :: Gen ProcessId
clientIds = Gen.constant Client

timestamp :: Integer -> Gen Integer
timestamp len = do
  num <- Gen.integral (Range.linear 0 (len * ticksPerSecond))
  return $ num

-- Technically, a Set of processid should be activated at each timestep.
-- But, at the moment, clocks are instantanious and global, wheras they should
-- be per process, really.

-- The number of "seconds" that we run for.
simLength :: Integral a => a
simLength = 30
ticksPerSecond :: Integer
ticksPerSecond = 1000
eventsPerSecond :: Integer
eventsPerSecond = 1

aPeerName :: Gen PeerName
aPeerName = PeerName <$> Gen.string (Range.linear 1 4) Gen.lower

peers :: Int -> Gen (PeerMap)
peers npeers = Bimap.fromList <$> (zip <$> names <*> (map (IdFor . hash) <$> names))
  where
    possibleNames = pure $ map (PeerName . show) ([0..] :: [Integer])
    names = take <$> Gen.int (Range.singleton npeers) <*> possibleNames

processSchedule :: Gen ProcessSchedule
processSchedule = Gen.shrink shrinkByMergingNeighbours gen
  where
  gen = Gen.list (Range.linear 0 (fromInteger $ simLength * eventsPerSecond))
                    ((,) <$> interArrivalTime eventsPerSecond <*> processEvent)

interArrivalTime :: Integer -> Gen Integer
interArrivalTime evPerSecond = do
  p <- Gen.float (Range.linearFrac 0.0 1.0)
  let t = -log (1.0 - p) * invlambda
  return $ round t
  where
  invlambda = fromIntegral ticksPerSecond / fromIntegral evPerSecond

shrinkByMergingNeighbours :: ProcessSchedule -> [ProcessSchedule]
shrinkByMergingNeighbours s = join $ map go $ List.inits s `zip` List.tails s
  where
  go (pfx, (t0, a) : (t1, _) : rest) = [pfx ++ (t0+t1, a) : rest]
  go (_, _) = []

inboxAndTick :: Gen ProcessSchedule
inboxAndTick = do
  t0 <- interArrivalTime 1
  t1 <- interArrivalTime 1
  return [(t0+1, Clock), (t1+1, Inbox)]


processEvent :: Gen ProcessEvent
processEvent = Gen.choice [ Gen.constant Clock, Gen.constant Inbox ]

schedule :: [ProcessId] -> Gen SystemSchedule
schedule = scheduleOf processSchedule

scheduleOf :: Gen ProcessSchedule -> [ProcessId] -> Gen SystemSchedule
scheduleOf proc pids = SystemSchedule . Map.fromList <$> allprocs
  where
    allprocs :: Gen [(ProcessId, [(Integer, ProcessEvent)])]
    allprocs = sequence $ flip map pids $ aProc
    aProc :: ProcessId -> Gen (ProcessId, [(Integer, ProcessEvent)])
    aProc p = (,) <$> Gen.constant p <*> proc

{-
We know that _only_ leaders will emit AppendEntries events, so rather than
inspecting the node state directly, we can listen for `AppendEntries` messages
from nodes, and record the source against the term.
-}
timeouts :: PeerMap -> Gen [Integer]
timeouts allPeers = Gen.list (Range.constant nnodes nnodes) $ ((+ 2500) <$> timestamp 1)
  where
  nnodes = Bimap.size allPeers

peerCount :: Gen Int
peerCount = Gen.int (Range.linear 1 5)

runSimulation :: (Show req, Show resp, Show st) => PeerMap -> [Integer] -> SystemSchedule -> req -> ModelFun st req resp -> st -> String -> PropertyT IO [MessageEvent st req resp]
runSimulation allPeers ts (SystemSchedule sched) aCmd modelFn initSt fname = do
      footnoteShow $ "Logging to: " ++ fname

      let byProc = map (\(p, s) -> map (\(t, e) -> (t, p, e)) $ toAbsTimes s) $ Map.toList sched :: [[(Integer,ProcessId,ProcessEvent)]]

      let toRun = List.mergeAll byProc
      -- footnoteShow toRun

      assert $ List.length toRun == 0 || List.isSorted (map (\(t,_,_) -> t) toRun)

      let network = Network allNodes (ClientSim Nothing 0 aCmd) 0
      evalIO $ do
        runFileLoggingT fname $ do
          $(logDebug) $ Text.pack $ ppShow ("allPeers", allPeers)
          $(logDebug) $ Text.pack $ ppShow ("timeouts", ts)
          $(logDebug) $ Text.pack $ ppShow ("sched", sched)
        let simulation = forM toRun $ \(t, p, ev) -> do
              msgs <- simulateIteration allPeers t p ev
              return msgs
        (msgs, network') <- State.runStateT (runFileLoggingT fname simulation) network
        runFileLoggingT fname $ do
          $(logDebug) $ Text.pack $ ppShow ("Network", network')
        return $ mconcat $ msgs

  where
    allNodes = Map.fromList $ fmap (\(p, t) -> (p, makeNode allPeers p t modelFn initSt)) $ zip (Bimap.keys allPeers) (map (% ticksPerSecond) ts)

    toAbsTimes :: [(Integer, a)] -> [(Integer, a)]
    toAbsTimes byInterval =
      let lefts = scanl (+) 0 $ map fst byInterval
          rights = map snd byInterval
      in
      lefts `zip` rights


prop_leaderElectionOnlyElectsOneLeaderPerTerm :: HasCallStack => Property
prop_leaderElectionOnlyElectsOneLeaderPerTerm = leaderElectionOnlyElectsOneLeaderPerTerm

leaderElectionOnlyElectsOneLeaderPerTerm :: (HasCallStack) => Property
leaderElectionOnlyElectsOneLeaderPerTerm = property $ do
      n <- forAll $ peerCount
      allPeers <- forAll $ peers n
      ts <- forAll $ timeouts allPeers
      sched <- forAll $ schedule $ nodeProcs allPeers
      let h = hash $ show (allPeers, ts, sched)
      let fname = "/tmp/prop_leaderElectionOnlyElectsOneLeaderPerTerm_" ++ show h

      messages <- runSimulation allPeers ts sched Bing bingBongModel 0 fname :: PropertyT IO [MessageEvent Int BingBongReq BingBongRet]

      let allLeaders = leadersByTerm messages

      test $ do
        footnoteShow $ ("Leaders by term", allLeaders)
        forM_ (Map.toList allLeaders) $ \(_term, leaders) -> do
          assert $ 1 >= Set.size leaders

  where
      leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ map leadersOf events
        -- let allLeaders = List.foldl' ... $
      leadersOf (MessageOut _mid sender (PeerRequest _ (AppendEntries aer) _)) =
          Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
      leadersOf _ = Map.empty

prop_bongsAreMonotonic :: HasCallStack => Property
prop_bongsAreMonotonic = bongsAreMonotonic


bongsAreMonotonic :: HasCallStack => Property
bongsAreMonotonic = property $ do
      n <- forAll $ peerCount
      allPeers <- forAll $ peers n
      ts <- forAll $ timeouts allPeers
      sched <- forAll $ schedule $ nodeProcs allPeers <|> clientProcs
      let h = hash $ show (allPeers, ts, sched)
      let fname = "/tmp/prop_bongsAreMonotonic" ++ show h

      messages <- runSimulation allPeers ts sched Bing bingBongModel 0 fname :: PropertyT IO [MessageEvent Int BingBongReq BingBongRet]
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

eventuallyElectsLeader :: HasCallStack => Property
eventuallyElectsLeader = property $ do
  -- we rely on AppendEntries items to guage when an election has happened.
  n <- forAll $ Gen.int (Range.linear 2 23)
  allPeers <- forAll $ peers n
  let ts = replicate n 2000
  let h = hash $ show (allPeers, ts)
  let fname = "/tmp/prop_eventuallyElectsLeader_" ++ show n ++ "_" ++ show h

  messages <- simulateUntil allPeers ts fname Bing bingBongModel 0 $ not . Map.null . mconcat . map leadersOf
  let _ = messages :: [MessageEvent Int BingBongReq BingBongRet]

  let allLeaders = leadersByTerm messages

  test $ do
    footnoteShow $ ("Leaders by term", allLeaders)
    assert $ Map.size allLeaders > 0

  where
    leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ map leadersOf events
      -- let allLeaders = List.foldl' ... $
    leadersOf (MessageOut _mid sender (PeerRequest _ (AppendEntries aer) _)) =
        Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
    leadersOf _ = Map.empty

eventuallyRepliesToClient :: HasCallStack => Property
eventuallyRepliesToClient = property $ do
  -- we rely on AppendEntries items to guage when an election has happened.
  n <- forAll $ Gen.int (Range.linear 2 23)
  allPeers <- forAll $ peers n
  let ts = replicate n 2000
  let h = hash $ show (allPeers, ts)
  let fname = "/tmp/prop_eventuallyRepliesToClient_" ++ show n ++ "_" ++ show h

  messages <- simulateUntil allPeers ts fname Bing bingBongModel 0 $ ((>0) . length . filter hasClientReply)
  let _ = messages :: [MessageEvent Int BingBongReq BingBongRet]

  let responses = filter hasClientReply messages

  test $ do
    footnoteShow $ ("Responses", responses)
    assert $ length responses > 0

  where
    hasClientReply :: MessageEvent st req resp -> Bool
    hasClientReply (MessageOut _ _ (Reply _ (Right _))) = True
    hasClientReply _ = False

simulateUntil :: (Show req, Show resp, Show st)
              => PeerMap -> [Integer] -> String -> req -> ModelFun st req resp -> st -> ([MessageEvent st req resp] -> Bool) -> PropertyT IO [MessageEvent st req resp]
simulateUntil allPeers ts fname aCmd modelFn initSt enough = do
  footnoteShow $ "Logging to: " ++ fname

  let sched = periodicSched
  let byProc = map (\(p, s) -> map (\(t, e) -> (t, p, e)) $ toAbsTimes $ cycle s)
                $ filter ((>0) . length . snd)
                $ Map.toList sched :: [[(Integer,ProcessId,ProcessEvent)]]

  let toRun = List.takeWhile (\(t, _, _) -> t < ticksPerSecond * simLength) $ List.mergeAll byProc
  assert $ List.length toRun == 0 || List.isSorted (map (\(t,_,_) -> t) toRun)

  let network = Network allNodes (ClientSim Nothing 0 aCmd) 0

  evalIO $ do
    runFileLoggingT fname $ do
      $(logDebug) $ Text.pack $ ppShow ("allPeers", allPeers)
      $(logDebug) $ Text.pack $ ppShow ("timeouts", ts)
      $(logDebug) $ Text.pack $ ppShow ("sched", sched)

    let runSched l = case l of
          (t, p, ev) : rest -> do
            $(logDebugSH) ("Iterate at", t, p)
            msgs <- simulateIteration allPeers t p ev
            $(logDebugSH) ("msgs", msgs)
            $(logDebugSH) ("enough", enough msgs)
            if not (enough msgs)
            then do
              $(logDebugSH) ("Repeat at", t, p)
              res <- runSched rest
              return $ msgs ++ res
            else do
              $(logDebugSH) ("Finished at", t, p)
              return msgs
          [] -> return []

    (msgs, network') <- State.runStateT (runFileLoggingT fname $ runSched toRun) network
    runFileLoggingT fname $ do
      $(logDebug) $ Text.pack $ ppShow ("toRun", toRun)
      $(logDebug) $ Text.pack $ ppShow ("Network", network')
      $(logDebug) $ Text.pack $ ppShow ("Messages", msgs)
    return $ msgs

    where
    allNodes = Map.fromList $ fmap (\(p, t) -> (p, makeNode allPeers p t modelFn initSt)) $ zip (Bimap.keys allPeers) (map (% ticksPerSecond) ts)

    periodicSched :: Map ProcessId [(Integer, ProcessEvent)]
    periodicSched = Map.fromList $ zip processes $ map sched [1000..]
      where
        sched :: Integer -> [(Integer, ProcessEvent)]
        sched n = [(0, Clock), (n, Inbox)]

    processes = (fmap Node $ Bimap.keys allPeers) ++ [Client]
    toAbsTimes :: [(Integer, a)] -> [(Integer, a)]
    toAbsTimes byInterval =
      let lefts = scanl (+) 0 $ map fst byInterval
          rights = map snd byInterval
      in
      lefts `zip` rights


-- mechanics

nextId :: MonadState (Network st req resp) m => m (IdFor a)
nextId = IdFor <$> (idCounter <<%= succ)

simulateIteration :: (HasCallStack, MonadLogger m, MonadState (Network st req resp) m, Show req, Show resp)
                  => PeerMap
                  -> Integer
                  -> ProcessId
                  -> ProcessEvent
                  -> m [MessageEvent st req resp]
simulateIteration _ n (Node name) Clock = do
    let t = n % ticksPerSecond
    mid <- nextId
    $(logDebugSH) ("Clock", name, t)
    (nodes . ix name . nodeInbox) %= (|> (mid, ClockTick t))
    return []

simulateIteration allPeers _t (Node name) Inbox = do
    simulateStep allPeers name

simulateIteration allPeers n Client Clock = do
    let t = n % ticksPerSecond
    cmd <- use (client . newCommand)
    rid <- IdFor <$> (client . sentReqId  <<%= succ)
    mid <- nextId
    use client >>= \st -> $(logDebugSH) ("simulateIteration", Client, rid, st)
    lastSeen <- use (client . lastSeenLeader)
    let peer = fromMaybe (Bimap.keys allPeers !!  (fromInteger n `mod` Bimap.size allPeers)) lastSeen
    $(logDebugSH) ("Client", peer, lastSeen, rid, t)
    (nodes . ix peer . nodeInbox) %= (|> (mid, ClientRequest rid cmd))
    return []
simulateIteration _allPeers _n Client Inbox = return []

getQuiesecent :: (Show req, MonadLogger m, MonadState (Network st req resp) m) => PeerName -> m Bool
getQuiesecent name = do
  inboxes <- toListOf (nodes . ix name . nodeInbox) <$> get
  $(logDebugSH) ("Inbox", name, inboxes)
  return $ all Seq.null $ inboxes


simulateStep :: forall st req resp m . (Show req, Show resp, HasCallStack, MonadLogger m, MonadState (Network st req resp) m) => PeerMap -> PeerName -> m [MessageEvent st req resp]
simulateStep allPeers thisNode = do
  node <- fromMaybe (error $ "No node: " ++ show thisNode) <$> preview (nodes . ix thisNode) <$> get :: m (NodeSim st req resp)
  let inbox = view nodeInbox node :: Seq (IdFor (MessageEvent st req resp), InboxItem st req resp)
  nodes . ix thisNode . nodeInbox .= Seq.empty

  $(logDebugSH) ("Run", thisNode, inbox)
  let actions = flip traverse_ inbox $ \(mid, action) -> do
                  $(logDebugSH) ("apply", thisNode, mid, action)
                  applyState allPeers action

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

        pendingCb <- use (nodes . ix dst . nodePendingResponses . ix thisNode)
        case Seq.viewl pendingCb of
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
  sendClientReply clientId m@(Right _) = do
        st <- use client
        $(logDebugSH) (thisNode, clientId, "sendClientReply", m, st)

nameOfPeerId :: PeerMap -> IdFor PeerResponse -> PeerName
nameOfPeerId m name = m Bimap.!> name

peerIdOfName :: PeerMap -> PeerName -> IdFor PeerResponse
peerIdOfName m name = m Bimap.! name

-- Driver bits


tests :: Group
tests = $$(discover)

spec :: Spec
spec = parallel $ do
  it "eventually elects a leader with a fixed schedule" $ do
    require $ eventuallyElectsLeader
  it "eventually replies to client with a fixed schedule" $ do
    require $ eventuallyRepliesToClient
  it "should elect only one leader per term " $ do
    require $ leaderElectionOnlyElectsOneLeaderPerTerm
  it "should produce monotonic bong responses" $ do
    require $ bongsAreMonotonic
