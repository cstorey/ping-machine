{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}

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
import qualified System.Environment as Env

import Lens.Micro.Platform

import Debug.Trace

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

newtype ClientName = ClientName String
  deriving (Show, Eq, Ord)

genClientName :: Int -> ClientName
genClientName = ClientName . prettyName

prettyName :: Int -> String
prettyName n = pretty
  where
  pretty = List.unfoldr unf $ succ n
  unf i | i > 0 = Just (toEnum $ (fromEnum 'a') + mod i 26, div i 26)
  unf _  = Nothing


type PeerMap = Bimap PeerName (IdFor PeerResponse)
type ClientMap resp = Bimap ClientName (IdFor (ClientResponse resp))

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

data ClientSim req resp = ClientSim {
  _lastSeenLeader :: Maybe PeerName
, _sentReqId :: Int
, _newCommand :: req
, _clientInbox :: Seq (PeerName, Either ClientError resp)
} deriving (Show)

data ClientHistItem req resp =
   ClientHistRequest ClientName PeerName req
 | ClientHistResponse ClientName PeerName (Either ClientError resp)
  deriving (Show)

data Network st req resp = Network {
  _nodes :: Map PeerName (NodeSim st req resp)
, _clients :: Map ClientName (ClientSim req resp)
, _idCounter :: Int
, _clientMap :: ClientMap resp
, _peerMapping :: PeerMap
, _allMessages :: Seq (MessageEvent st req resp)
, _clientHistory :: Seq (ClientHistItem req resp)
} deriving (Show)

makeLenses ''NodeSim
makeLenses ''ClientSim
makeLenses ''Network

makeNode :: PeerMap -> PeerName -> Time -> Model st req resp -> NodeSim st req resp
makeNode allPeers self elTimeout model = NodeSim newnodeEnv mkRaftState Seq.empty Map.empty
  where
    newnodeEnv = mkProtocolEnv self (Set.difference names $ Set.singleton self) elTimeout (elTimeout / 3.0) model
    names = Set.fromList $ Bimap.keys allPeers


applyState :: (HasCallStack, Show req, Show resp, MonadState (Network st req resp) m)
           => InboxItem st req resp
           -> m (ProtoStateMachine st req resp ())
applyState (ClockTick t) = do
  pure $ processTick () $ Tick t

applyState (RequestFromPeer sender req) = do
  (processPeerRequestMessage req <$> peerIdOfName sender)

applyState (ResponseFromPeer _sender (WaitingCallback f) resp) = pure $ f resp

applyState (ClientRequest rid cmd) = do
  pure $ processClientReqRespMessage cmd rid

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
    Client ClientName
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
clientProcs :: Int -> [ProcessId]
clientProcs n = [Client $ genClientName i | i <- [0..n] ]

peerName :: PeerMap -> Gen PeerName
peerName allPeers = Gen.element $ Bimap.keys allPeers

clientCommand :: Gen BingBongReq
clientCommand = Gen.choice
  [ Gen.constant Bing
  , Gen.constant Ping
  ]

serverIds :: PeerMap -> Gen ProcessId
serverIds allPeers = Node <$> peerName allPeers

clientIds :: Int -> Gen ProcessId
clientIds n = Client . genClientName <$> Gen.integral (Range.linear 0 n)

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

peers :: Range Int -> Gen (PeerMap)
peers numGen = Bimap.fromList <$> (zip <$> names <*> (map (IdFor . hash) <$> names))
  where
    possibleNames = pure $ map (PeerName . prettyName) [0..]
    names = take <$> Gen.int numGen <*> possibleNames

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

randomSchedule :: [ProcessId] -> Gen SystemSchedule
randomSchedule = scheduleOf processSchedule

periodicSchedule :: [ProcessId] -> Gen SystemSchedule
periodicSchedule processes = pure $ SystemSchedule $ Map.fromList $ zip processes $ map sched [1000..]
  where
    sched :: Integer -> [(Integer, ProcessEvent)]
    sched n = [(0, Clock), (n, Inbox)]

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

peerCount :: Range Int
peerCount = Range.linear 1 5

clientCount :: Gen Int
clientCount = Gen.int (Range.linear 1 5)


configs :: String -> Range Int -> ([ProcessId] -> Gen SystemSchedule) -> Gen SimulationConfig
configs name numPeers scheduler = do
    mypeers <- peers numPeers
    cs <- clientCount
    sched <- scheduler $ nodeProcs mypeers <|> clientProcs cs
    return $ SimulationConfig name cs mypeers sched

runSimulation :: (Show req, Show resp, Show st) => SimulationConfig -> req -> Model st req resp -> PropertyT IO (Network st req resp)
runSimulation config aCmd model = do
      let fname = logFileName config
      footnoteShow $ "Logging to: " ++ fname

      let SystemSchedule sched = simSchedule config
      let byProc = map (\(p, s) -> map (\(t, e) -> (t, p, e)) $ toAbsTimes s) $ Map.toList sched :: [[(Integer,ProcessId,ProcessEvent)]]

      let toRun = List.mergeAll byProc
      -- footnoteShow toRun

      assert $ List.length toRun == 0 || List.isSorted (map (\(t,_,_) -> t) toRun)

      let network = networkOfConfig config model aCmd
      evalIO $ do
        runFileLoggingT fname $ do
          $(logDebug) $ Text.pack $ ppShow ("config", config)
        let simulation = forM_ toRun $ \(t, p, ev) -> simulateIteration t p ev
        ((), network') <- State.runStateT (runFileLoggingT fname simulation) network
        runFileLoggingT fname $ do
          $(logDebug) $ Text.pack $ ppShow ("Network", network')
        return network'

  where
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
      config <- forAll $ configs "leaderElectionOnlyElectsOneLeaderPerTerm" (Range.linear 2 23) randomSchedule
      network <- runSimulation config Bing bingBongModel

      let allLeaders = leadersByTerm $ view allMessages network

      test $ do
        footnoteShow $ ("Leaders by term", allLeaders)
        forM_ (Map.toList allLeaders) $ \(_term, leaders) -> do
          assert $ 1 >= Set.size leaders

  where
      leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ fmap leadersOf events
      leadersOf (MessageOut _mid sender (PeerRequest _ (AppendEntries aer) _)) =
          Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
      leadersOf _ = Map.empty

prop_bongsAreMonotonic :: HasCallStack => Property
prop_bongsAreMonotonic = bongsAreMonotonic


bongsAreMonotonic :: HasCallStack => Property
bongsAreMonotonic = property $ do
      config <- forAll $ configs "bongsAreMonotonic" (Range.linear 2 23) randomSchedule
      network <- runSimulation config Bing bingBongModel
      let respValues = foldr findResponse [] $ view clientHistory network
      test $ do
        runFileLoggingT (logFileName config) $ do
          -- $(logDebug) $ Text.pack $ ppShow ("messages", messages)
          $(logDebug) $ Text.pack $ ppShow ("responses", respValues)
          $(logDebug) $ Text.pack $ ppShow ("okay?", respValues == List.sort respValues)
          -- footnoteShow messages
        respValues === List.sort respValues

  where
    findResponse (ClientHistResponse _i _peer (Right val)) r = val : r
    findResponse _ r = r

data SimulationConfig = SimulationConfig {
  simName :: String
, simNClients :: Int
, simPeers :: PeerMap
, simSchedule :: SystemSchedule
} deriving (Show)

instance Hashable SimulationConfig where
  hashWithSalt salt v =
    hashWithSalt salt
         ( simName v
         , simNClients v
         , Bimap.toList $ simPeers v
         )

logFileName :: SimulationConfig -> String
logFileName config = "/tmp/prop_" ++ simName config ++ "_" ++ show npeers ++ "_" ++ show h
  where
  npeers = (Bimap.size . simPeers $ config)
  h = hash config

eventuallyElectsLeader :: HasCallStack => Property
eventuallyElectsLeader = property $ do
  -- we rely on AppendEntries items to guage when an election has happened.
  -- hence, we need at least two nodes.
  config <- forAll $ configs "eventuallyElectsLeader" (Range.linear 2 93) periodicSchedule

  network <- simulateUntil config Bing bingBongModel isDone

  let allLeaders = leadersByTerm $ toList $ view allMessages network

  test $ do
    footnoteShow $ ("Leaders by term", allLeaders)
    assert $ Map.size allLeaders > 0

  where
    isDone = not . Map.null . foldl' mappend mempty . fmap leadersOf . view allMessages
    leadersByTerm events = foldl' (Map.unionWith Set.union) Map.empty $ map leadersOf events
      -- let allLeaders = List.foldl' ... $
    leadersOf (MessageOut _mid sender (PeerRequest _ (AppendEntries aer) _)) =
        Map.singleton (aeLeaderTerm aer) $ Set.singleton sender
    leadersOf _ = Map.empty

eventuallyRepliesToClient :: HasCallStack => Property
eventuallyRepliesToClient = property $ do
  -- we rely on AppendEntries items to guage when an election has happened.
  config <- forAll $ configs "eventuallyRepliesToClient" (Range.linear 2 23) periodicSchedule
  network <- simulateUntil config Bing bingBongModel isDone

  let responses = filter hasClientReply . toList $ view clientHistory network

  test $ do
    footnoteShow $ ("ClientHistory", view clientHistory network)
    footnoteShow $ ("Responses", responses)
    assert $ length responses > 0

  where
    isDone = (>0) . Seq.length . mfilter hasClientReply . view clientHistory
    hasClientReply (ClientHistResponse _ _ (Right _)) = True
    hasClientReply _ = False

networkOfConfig :: SimulationConfig -> Model st req resp -> req -> Network st req resp
networkOfConfig config model aCmd =
  Network allNodes allClients 0 myclientMap allPeers Seq.empty Seq.empty
  where
  allPeers = simPeers config
  allNodes = Map.fromList $ fmap (\(p, t) -> (p, makeNode allPeers p t model)) $ zip (Bimap.keys allPeers) (map (% ticksPerSecond) $ cycle [2000])
  allClients = Map.fromList [(genClientName i, (ClientSim Nothing 0 aCmd Seq.empty)) | i <- [0..simNClients config] ]
  myclientMap = Bimap.fromList $ Map.keys allClients `zip` fmap IdFor [10000..]


simulateUntil :: (Show req, Show resp, Show st)
              => SimulationConfig -> req -> Model st req resp -> (Network st req resp -> Bool) -> PropertyT IO (Network st req resp)
simulateUntil config aCmd model enough = do
  let fname = logFileName config
  footnoteShow $ "Logging to: " ++ fname

  let SystemSchedule sched = simSchedule config
  let byProc = map (\(p, s) -> map (\(t, e) -> (t, p, e)) $ toAbsTimes $ cycle s)
                $ filter ((>0) . length . snd)
                $ Map.toList sched :: [[(Integer,ProcessId,ProcessEvent)]]

  let toRun = List.takeWhile (\(t, _, _) -> t < ticksPerSecond * simLength ) $ List.mergeAll byProc
  assert $ List.length toRun == 0 || List.isSorted (map (\(t,_,_) -> t) toRun)

  let network = networkOfConfig config model aCmd
  debugp <- evalIO $ do
    maybe False (/= "") <$> Env.lookupEnv "STUFF_DEBUG"

  evalIO $ do
    when debugp $ runFileLoggingT fname $ do
      $(logDebug) $ Text.pack $ ppShow ("config", config)
      $(logDebug) $ Text.pack $ ppShow ("sched", sched)

    ((), network') <- State.runStateT (runFileLoggingT fname $ runSched toRun) network
    when debugp $ runFileLoggingT fname $ do
      $(logDebug) $ Text.pack $ ppShow ("Network", network')
      $(logDebug) $ Text.pack $ ppShow ("Messages", view allMessages network')
    return $ network'

    where

    runSched l = case l of
          (t, p, ev) : rest -> do
            $(logDebugSH) ("Iterate at", t, p)
            simulateIteration t p ev
            net <- get
            $(logDebugSH) ("enough", enough net)
            if not (enough net)
            then do
              $(logDebugSH) ("Repeat at", t, p)
              runSched rest
            else do
              $(logDebugSH) ("Finished at", t, p)
          [] -> return ()

    toAbsTimes :: [(Integer, a)] -> [(Integer, a)]
    toAbsTimes byInterval =
      let lefts = scanl (+) 0 $ map fst byInterval
          rights = map snd byInterval
      in
      lefts `zip` rights


-- mechanics

nextId :: MonadState (Network st req resp) m => m (IdFor a)
nextId = IdFor <$> (idCounter <<%= succ)

simulateIteration :: forall m st req resp . (HasCallStack, MonadLogger m, MonadState (Network st req resp) m, Show req, Show resp)
                  => Integer
                  -> ProcessId
                  -> ProcessEvent
                  -> m ()
simulateIteration n (Node name) Clock = do
    let t = n % ticksPerSecond
    mid <- nextId
    $(logDebugSH) ("Clock", name, t)
    (nodes . ix name . nodeInbox) %= (|> (mid, ClockTick t))

simulateIteration _t (Node name) Inbox = do
    msgs <- simulateStep name
    allMessages %= (`mappend` msgs)

simulateIteration n (Client i) Clock = do
    rid <- clientIdOfName i
    allPeers <- use peerMapping
    r <- zoomClient i $ do
      cmd <- use newCommand
      lastSeen <- use lastSeenLeader
      let peer = fromMaybe (Bimap.keys allPeers !!  (fromInteger n `mod` Bimap.size allPeers)) lastSeen
      return (peer, cmd)

    case r of
      Just  (peer, req) -> do
        mid <- nextId
        $(logDebugSH) ("simulateIteration", Client i, Clock, "sending", mid, ClientRequest rid req)

        (nodes . ix peer . nodeInbox) %= (|> (mid, ClientRequest rid req))

        clientHistory %= (|> ClientHistRequest i peer req)

      Nothing -> do
        traceShowM ("No client found for", i, rid)

simulateIteration t (Client i) Inbox = do
  use (clients . at i) >>= \st -> $(logDebugSH) ("pre simulateIteration", t, Client i, st)
  inbox <- clients . ix i . clientInbox <<.= Seq.empty
  use (clients . at i) >>= \st -> $(logDebugSH) ("post simulateIteration", t, Client i, st)
  clientHistory %= (`mappend` fmap (uncurry $ ClientHistResponse i) inbox)

zoomClient :: (MonadState (Network st req resp) m) => ClientName -> State.StateT (ClientSim req resp) m a -> m (Maybe a)
zoomClient i action = do
  clp <- preuse $ (clients . ix i)
  case clp of
    Just cl -> do
      (r, cl') <- State.runStateT action cl
      (clients . at i) .= Just cl'
      return $ Just r
    Nothing -> return Nothing

getQuiesecent :: (Show req, MonadLogger m, MonadState (Network st req resp) m) => PeerName -> m Bool
getQuiesecent name = do
  inboxes <- toListOf (nodes . ix name . nodeInbox) <$> get
  $(logDebugSH) ("Inbox", name, inboxes)
  return $ all Seq.null $ inboxes


simulateStep :: forall st req resp m . (Show req, Show resp, HasCallStack, MonadLogger m, MonadState (Network st req resp) m) => PeerName -> m (Seq (MessageEvent st req resp))
simulateStep thisNode = do
  node <- fromMaybe (error $ "No node: " ++ show thisNode) <$> preview (nodes . ix thisNode) <$> get :: m (NodeSim st req resp)
  let inbox = view nodeInbox node :: Seq (IdFor (MessageEvent st req resp), InboxItem st req resp)
  nodes . ix thisNode . nodeInbox .= Seq.empty

  $(logDebugSH) ("Run", thisNode, inbox)
  psmActions <- flip traverse inbox $ \(_, action) -> do applyState action

  let actions = traverse_ id psmActions

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

  let ins = fmap (\(mid, msg) -> MessageIn mid thisNode msg) $ inbox
  return $ ins `mappend` Seq.fromList outs
  where


  sendPeerRequest mid dst m cb= do
        $(logDebugSH) (unPeerName thisNode, "->", unPeerName dst, "PeerRequest", m)
        nodes . ix dst . nodeInbox %= (|> (mid, RequestFromPeer thisNode m))
        nodes . ix thisNode . nodePendingResponses %= (Map.insertWith (flip mappend) dst $ Seq.singleton $ WaitingCallback cb)
        do
          p <-  use $ nodes . ix thisNode . nodePendingResponses
          $(logDebugSH) (unPeerName thisNode, "post sendPeerRequest", "num pending callbacks", fmap Seq.length p)

  sendPeerReply mid peerId m = do
        dst <- nameOfPeerId peerId
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
        dst <- nameOfClientId clientId

        use (clients . at dst) >>= \st -> $(logDebugSH) (dst, thisNode, clientId, "pre sendClientReply", m, st)

        clients . ix dst . lastSeenLeader .= leaderp
        clients . ix dst . clientInbox %= (|> (thisNode, m))

        use (clients . at dst) >>= \st -> $(logDebugSH) (dst, thisNode, clientId, "post sendClientReply", m, st)

        return ()

  sendClientReply clientId m@(Right _) = do
        dst <- nameOfClientId clientId
        clients . ix dst . clientInbox %= (|> (thisNode, m))
        use (clients . at dst) >>= \st -> $(logDebugSH) (thisNode, clientId, "sendClientReply", m, st)

nameOfPeerId :: (HasCallStack, MonadState (Network st req resp) m) => IdFor PeerResponse -> m PeerName
nameOfPeerId name = do
  m <- use peerMapping
  return $ maybe (error $ "peer Not found: " ++ show name) id $ Bimap.lookupR name m

peerIdOfName :: (HasCallStack, MonadState (Network st req resp) m) => PeerName -> m (IdFor PeerResponse)
peerIdOfName name = do
  m <- use peerMapping
  return $ m Bimap.! name

nameOfClientId :: (HasCallStack, MonadState (Network st req resp) m) => IdFor (ClientResponse resp) -> m ClientName
nameOfClientId name = do
  m <- use clientMap
  let r = Bimap.lookupR name m
  return $ maybe (error $ "client Not found: " ++ show ("nameOfClientId", name, m, r)) id r

clientIdOfName :: (HasCallStack, MonadState (Network st req resp) m) => ClientName -> m (IdFor (ClientResponse resp))
clientIdOfName name = do
  m <- use clientMap
  return $ m Bimap.! name

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
