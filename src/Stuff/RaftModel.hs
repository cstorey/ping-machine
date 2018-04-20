{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

module Stuff.RaftModel
( ProcessorMessage(..)
, ProtoStateMachine(..)
, RaftState
, RaftRole
, ProtocolEnv
, mkProtocolEnv
, IdFor(..)
, processClientReqRespMessage
, processPeerRequestMessage
, processPeerResponseMessage
, processTick
, mkRaftState
)
where

import qualified Stuff.Proto as Proto
import qualified Stuff.Models as Models

import qualified Control.Monad.Trans.RWS.Strict as RWS
import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..))
import           Control.Monad.Reader.Class (MonadReader(..))
import           Control.Monad.Trans.State.Strict (State)
import qualified Control.Monad.Trans.State.Strict as State
import           Control.Monad.Logger (WriterLoggingT, MonadLogger, logDebug, logDebugSH, logWarnSH, logInfoSH)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import Data.Tuple (swap)
import Control.Monad
import Data.Functor.Identity (Identity)
import Lens.Micro.Platform
import qualified Data.Text as Text
import Data.Hashable (Hashable)

import GHC.Stack
import GHC.Generics (Generic)
import Stuff.Types

newtype IdFor a = IdFor Int
    deriving (Show, Eq, Ord, Generic)

instance Hashable (IdFor a)

type PeerSet = Set Proto.PeerName


data ProtocolEnv st req resp = ProtocolEnv {
    _selfId :: Proto.PeerName,
    _peerNames :: PeerSet,
    _electionTimeout :: Time,
    __appendEntriesPeriod :: Time
,   _model :: Models.Model st req resp
}

instance (Show req, Show resp) => Show (ProtocolEnv st req resp) where
  show _self = "ProtocolEnv"

makeLenses ''ProtocolEnv

data FollowerState = FollowerState {
    _lastLeaderHeartbeat :: Time
} deriving (Show)
makeLenses ''FollowerState

data CandidateState = CandidateState {
    _requestVoteSentAt :: Time
,   _votesForMe :: PeerSet
} deriving (Show)
makeLenses ''CandidateState

data LeaderFollowerState = LeaderFollowerState {
    _prevIdx :: Proto.LogIdx
,   _lastSent :: Proto.LogIdx
} deriving (Show)
makeLenses ''LeaderFollowerState

data LeaderState resp = LeaderState {
    _followers :: Map.Map Proto.PeerName LeaderFollowerState
,   _committed :: Maybe Proto.LogIdx
,   _pendingClientRequests :: Map.Map Proto.LogIdx (IdFor (Proto.ClientResponse resp))
} deriving (Show)
makeLenses ''LeaderState


data RaftRole resp =
    Follower FollowerState
  | Candidate CandidateState
  | Leader (LeaderState resp)
 deriving (Show)
data RaftState req resp = RaftState {
    _currentRole :: RaftRole resp
,   _currentTerm :: Proto.Term
,   _logEntries :: Map.Map Proto.LogIdx (Proto.LogEntry req)
,   _votedFor :: Maybe Proto.PeerName
,   _prevTickTime :: Time
,   _currentLeader :: Maybe Proto.PeerName
} deriving (Show)
makeLenses ''RaftState


data ProcessorMessage st req resp = Reply (IdFor (Proto.ClientResponse resp)) (Proto.ClientResponse resp)
    | PeerReply (IdFor Proto.PeerResponse) Proto.PeerResponse
    | PeerRequest Proto.PeerName (Proto.PeerRequest req)
  deriving (Show)

newtype ProtoStateMachine st req resp a = ProtoStateMachine {
    runProto :: (RWS.RWST (ProtocolEnv st req resp) [ProcessorMessage st req resp] (RaftState req resp) (WriterLoggingT Identity)) a
} deriving (Monad, Applicative, Functor,
            MonadState (RaftState req resp), MonadWriter [ProcessorMessage st req resp],
            MonadReader (ProtocolEnv st req resp), MonadLogger)

succIdx :: Proto.LogIdx -> Proto.LogIdx
succIdx (Proto.LogIdx Nothing) = Proto.LogIdx $ Just 0
succIdx (Proto.LogIdx (Just x)) = Proto.LogIdx $ Just $ succ x

mkProtocolEnv :: Proto.PeerName -> PeerSet -> Time -> Time -> Models.Model st req resp -> ProtocolEnv st req resp
mkProtocolEnv = ProtocolEnv

newFollower :: RaftRole resp
newFollower = Follower $ FollowerState 0

mkRaftState :: RaftState req resp
mkRaftState = RaftState newFollower 0 Map.empty Nothing 0 Nothing

-- Log operations
appendToLog :: req -> ProtoStateMachine st req resp Proto.LogIdx
appendToLog command = do
    thisTerm <- use currentTerm
    let entry = Proto.LogEntry thisTerm command
    (_, logIdx) <- getPrevLogTermIdx
    let idx = succIdx logIdx
    logEntries %= Map.insert idx entry
    return idx

getPrevLogTermIdx :: HasCallStack => ProtoStateMachine st req resp (Proto.Term, Proto.LogIdx)
getPrevLogTermIdx = do
    myLog <- use logEntries
    return $ maybe (0, Proto.LogIdx $ Nothing) (\(idx, it) -> (Proto.logTerm it, idx)) $ Map.lookupMax myLog

readLogEntry :: MonadState (RaftState req resp) m
             => Proto.LogIdx
             -> m (Maybe (Proto.LogEntry req))
readLogEntry assumedHeadIdx = Map.lookup assumedHeadIdx <$> use logEntries

appendLogAfter :: (MonadLogger m, Show req,
                   MonadReader (ProtocolEnv st req resp) m,
                   MonadState (RaftState req resp) m) =>
                  Proto.LogIdx -> Map.Map Proto.LogIdx (Proto.LogEntry req) -> m ()

appendLogAfter assumedHeadIdx newEntries = do
  entries <- use logEntries
  let prefix   = Map.takeWhileAntitone (<= assumedHeadIdx) entries
  $(logDebug) $ Text.pack (
      "Map.takeWhileAntitone (<= " ++ show assumedHeadIdx ++ ") " ++ show entries
      ++ " => " ++ show prefix)

  self <- view selfId
  $(logDebugSH) (
      Proto.unPeerName self,
      "Have", map Proto.unLogIdx $ Map.keys entries,
      "new prefix", map Proto.unLogIdx $ Map.keys prefix,
      "adding", map Proto.unLogIdx $ Map.keys newEntries)
  let entries' = Map.union prefix newEntries

  $(logDebugSH) ( Proto.unPeerName self, "Now", map Proto.unLogIdx $ Map.keys entries')
  logEntries .= entries'


readLogUptoInclusive :: MonadState (RaftState req resp) m =>
                                   Proto.LogIdx
                                   -> m (Map.Map Proto.LogIdx (Proto.LogEntry req))
readLogUptoInclusive reqIdx = do
  prev <- Map.takeWhileAntitone (<= reqIdx) <$> use logEntries
  pure prev

readItemsAfter :: (MonadLogger m,
                         MonadState (RaftState req resp) m) =>
                        Proto.LogIdx -> m (Map.Map Proto.LogIdx (Proto.LogEntry req))
readItemsAfter peerLastSent = do
        entries <- use logEntries
        -- Send everything _after_ their previous index
        let toSend = Map.dropWhileAntitone (<= peerLastSent) entries
        $(logDebugSH) ("sendingitems", Map.size toSend, "out of", Map.size entries)
        return toSend



-- handlers

processClientReqRespMessage :: (Show req, Show resp) => req -> IdFor (Proto.ClientResponse resp) -> ProtoStateMachine st req resp ()
processClientReqRespMessage command pendingResponse = do
    $(logDebugSH) ("processClientReqRespMessage", command, pendingResponse)
    role <- use currentRole
    case role of
        Leader leader -> do
            idx <- appendToLog command
            $(logDebugSH) ("processClientReqRespMessage accepting", command, pendingResponse, "index", Proto.unLogIdx idx)
            leader' <- recordPendingClientRequest idx leader
            leader'' <- replicatePendingEntriesToFollowers leader'
            currentRole .= Leader leader''
            $(logDebugSH) ("processClientReqRespMessage post", leader'')
        _ -> do
            $(logDebugSH) ("processClientReqRespMessage refusing", command, pendingResponse)
            refuseClientRequest

    where
    refuseClientRequest = do
        myLeader <- use currentLeader
        $(logDebugSH) "Not leader"
        tell [Reply pendingResponse $ Left $ Proto.NotLeader myLeader]

    recordPendingClientRequest idx leader = do
        return $ over pendingClientRequests (Map.insert idx pendingResponse) leader


observeTerm :: HasCallStack => Proto.Term -> ProtoStateMachine st req resp ()
observeTerm laterTerm = do
    thisTerm <- use currentTerm
    formerRole <- use currentRole

    when (thisTerm < laterTerm) $ do
        $(logDebugSH) ("Later term observed", laterTerm, " > ", thisTerm)
        currentTerm .= laterTerm
        votedFor .= Nothing

        case formerRole of
            Leader leader -> whenLeader leader
            _ -> return ()


    void $ stepDown
    where
        whenLeader :: HasCallStack => LeaderState resp -> ProtoStateMachine st req resp ()
        whenLeader leader = do
            let pending = view pendingClientRequests leader
            $(logDebugSH) ("Nacking requests", pending)
            forM_ (Map.toList pending) $ \(_, clid) -> do
                -- We should really actually run a state machine here. But ...
                tell [Reply clid $ Left $ Proto.NotLeader $ Nothing]

stepDown :: HasCallStack => ProtoStateMachine st req resp FollowerState
stepDown = do
    prevTick <- use prevTickTime
    let st = FollowerState prevTick
    let role' = Follower $ st
    currentRole .= role'
    return st

getMajority :: HasCallStack => ProtoStateMachine st req resp Int
getMajority = do
    memberCount <- succ . length <$> view peerNames
    return $ succ (memberCount `div` 2)

processPeerRequestMessage :: forall st req resp . (HasCallStack, Show req)
                          => Proto.PeerRequest req
                          -> IdFor Proto.PeerResponse
                          -> ProtoStateMachine st req resp ()
processPeerRequestMessage (Proto.RequestVote req) sender = do
    -- let x = (RequestVoteReq candidateTerm candidateName candidateIdx);
    thisTerm <- use currentTerm
    vote <- use votedFor
    let candidateTerm = Proto.rvTerm req
    (_prevTerm, logIdx) <- getPrevLogTermIdx
    observeTerm candidateTerm
    case (candidateTerm `compare` thisTerm, Proto.rvHead req `compare` logIdx, vote) of
        (LT, _, _) -> do
            $(logDebugSH) ("Refusing vote; candidate term", candidateTerm, "earlier than mine", thisTerm)
            refuseVote thisTerm
        (_, LT, _) -> do
            $(logDebugSH) ("Refusing vote; candidate term", candidateTerm, ">= mine", thisTerm, "their head", Proto.rvHead req, "< ours", logIdx)
            refuseVote candidateTerm
        (_, _, Just v) -> do
            $(logDebugSH) ("Refusing vote; already voted for ", v)
            refuseVote candidateTerm
        (_, _, _) -> do
            $(logDebugSH) ("Granting vote; candidate term", candidateTerm, "later than mine", thisTerm, "their head", Proto.rvHead req, ">= ours", logIdx)
            grantVote candidateTerm

    where
        refuseVote :: HasCallStack => Proto.Term -> ProtoStateMachine st req resp ()
        refuseVote thisTerm = tell [PeerReply sender $ Proto.VoteResult thisTerm False]
        grantVote :: HasCallStack => Proto.Term -> ProtoStateMachine st req resp ()
        grantVote thisTerm = do
            votedFor .= Just (Proto.rvName req)
            tell [PeerReply sender $ Proto.VoteResult thisTerm True]

processPeerRequestMessage
    _msg@(Proto.AppendEntries (Proto.AppendEntriesReq leaderTerm leaderName assumedHeadTerm assumedHeadIdx newEntries)) reqId = do
    -- prevTick <- prevTickTime <$> get
    $(logDebugSH) ("<- Append Entries", _msg)

    thisTerm <- use currentTerm
    (_prevTerm, logHeadIdx) <- getPrevLogTermIdx
    if leaderTerm < thisTerm
    then do
        return ()
        $(logDebugSH) ("Refuse appendentries", leaderTerm, " < ", thisTerm)
        refuseAppendEntries thisTerm logHeadIdx
    else
        if leaderTerm > thisTerm
        then observeTerm leaderTerm
        else
        do
            role <- use currentRole
            case role of
                Follower follower -> do
                    whenFollower thisTerm follower
                Candidate _st -> do
                    follower <- stepDown
                    whenFollower thisTerm follower
                Leader _st -> do
                    error ("appendEntries recieved when leader? " ++ show _msg)

    where
        refuseAppendEntries :: HasCallStack => Proto.Term -> Proto.LogIdx -> ProtoStateMachine st req resp ()
        refuseAppendEntries thisTerm curHead = tell [PeerReply reqId $ Proto.AppendResult $ Proto.AppendEntriesResponse thisTerm False curHead]
        ackAppendEntries :: HasCallStack => Proto.Term -> Proto.LogIdx -> ProtoStateMachine st req resp ()
        ackAppendEntries thisTerm curHead = tell [PeerReply reqId $ Proto.AppendResult $ Proto.AppendEntriesResponse thisTerm True curHead]

        whenFollower thisTerm follower = do
            prevTick <- use prevTickTime
            let follower' = set lastLeaderHeartbeat prevTick follower
            currentRole .= Follower follower'

            (_prevTerm, logHeadIdx) <- getPrevLogTermIdx
            myEntry <- readLogEntry assumedHeadIdx
            case Proto.logTerm <$> myEntry of
                Just n | n /= assumedHeadTerm -> do
                    -- $(logDebugSH) ("Refusing as prev item was", myEntry, "wanted term", assumedHeadTerm)
                    refuseAppendEntries thisTerm logHeadIdx
                -- We need to refuse here iff it's ahead of our log
                Nothing | assumedHeadIdx > logHeadIdx -> do
                    $(logDebugSH) ("Refusing as prevIdx index", assumedHeadIdx, "ahead of our", logHeadIdx)
                    refuseAppendEntries thisTerm logHeadIdx
                _ -> do
                    $(logDebugSH) ("Appending from ", assumedHeadIdx, "was", logHeadIdx)

                    appendLogAfter assumedHeadIdx newEntries
                    currentLeader .= Just leaderName

                    (_prevTerm, newLogHead) <- getPrevLogTermIdx

                    ackAppendEntries thisTerm newLogHead


handleVoteResponse :: HasCallStack => Proto.PeerName -> Proto.PeerResponse -> ProtoStateMachine st req resp ()
handleVoteResponse sender _msg@(Proto.VoteResult peerTerm granted) = do
    $(logDebugSH) ("handleVoteResponse to", _msg)
    role <- use currentRole
    myTerm <- use currentTerm

    case role of
        _ | peerTerm > myTerm -> observeTerm peerTerm
        _ | peerTerm < myTerm -> $(logDebugSH) ("Ignoring vote for previous term")
        Candidate st -> do
          when granted $ do
            transitiontoLeaderWithEnoughVotes sender st
        _ -> do
            $(logDebugSH) ("vote recieved when non candidate?")

handleVoteResponse sender _msg = do
    me <- view selfId
    let msg =
            (Proto.unPeerName me , ": Unexpected response from " , sender ,
            " resp " , _msg)
    $(logWarnSH) msg
    void $ error $ show msg

transitiontoLeaderWithEnoughVotes :: Proto.PeerName
                                  -> CandidateState
                                  -> ProtoStateMachine st req resp ()
transitiontoLeaderWithEnoughVotes sender candidate = do
            let newCandidate = over votesForMe (Set.insert sender) candidate
            let currentVotes = view votesForMe newCandidate

            neededVotes <- getMajority

            myTerm <- use currentTerm
            $(logDebugSH) (
                "In term: " , myTerm , " needed: " , neededVotes , " have: " , currentVotes)

            let newRole = if length currentVotes >= neededVotes
                then Leader $ LeaderState Map.empty Nothing Map.empty
                else Candidate newCandidate

            currentRole .= newRole

handleAppendEntriesResponse :: forall st req resp . (HasCallStack, Show req)
                            => Proto.PeerName
                            -> Proto.PeerResponse
                            -> ProtoStateMachine st req resp ()
handleAppendEntriesResponse sender _msg@(Proto.AppendResult aer) = do
    $(logDebugSH) ("handleAppendEntriesResponse", _msg)
    role <- use currentRole
    case role of
        Leader leader -> do
            leader' <- whenLeader leader
            leader'' <- maybe (return leader') (ackPendingClientResponses leader' ) $ view committed leader'
            currentRole .= (Leader $ leader'')
        _st -> do
            $(logWarnSH) ("append response recieved when non leader?", _msg)
    where
    whenLeader leader = do
      let sentIdx = Proto.aerLogHead aer
      if Proto.aerSucceeded aer
      then do
          -- let followerPrevIdx' = Map.insert sender sentIdx $ view followerPrevIdx leader
          committedIdx <- findCommittedIndex leader

          $(logDebugSH) ("committed index should be", committedIdx)

          return $ leader
              & set (followers . ix sender . prevIdx) sentIdx
              & set committed committedIdx

      else do
          let toTry = Proto.aerLogHead aer
          $(logDebugSH) ("Retry peer " , sender , " at : " , toTry, "from", sentIdx)
          return $ leader
                    & set (followers . ix sender . prevIdx) toTry
                    & set (followers . ix sender . lastSent) toTry

    findCommittedIndex :: HasCallStack => LeaderState resp -> ProtoStateMachine st req resp (Maybe Proto.LogIdx)
    findCommittedIndex st = do
        majority <- getMajority
        -- Find items acked by a ajority of servers.
        -- So first derive the acked idxes in descending order
        (_, myIdx ) <- getPrevLogTermIdx
        let allIndexes = myIdx : toListOf (followers . each . prevIdx) st
        let known = List.sortBy (flip compare) $ allIndexes
        $(logDebugSH) ("Known follower indexes: " , known)
        return $ Maybe.listToMaybe $ List.drop (pred majority) known

    ackPendingClientResponses :: HasCallStack
                              => LeaderState resp -> Proto.LogIdx -> ProtoStateMachine st req resp (LeaderState resp)
    ackPendingClientResponses leader idx = do
        let pending = view pendingClientRequests leader
        let (canRespond, unCommitted) = Map.spanAntitone (<= idx) pending
        $(logDebugSH) ("committed index: " , idx , " pending: " , canRespond)

        $(logDebugSH) ("Responding to requests: " , canRespond)
        forM_ (Map.toList canRespond) $ \(reqIdx, clid) -> do
            -- We should really actually run a state machine here. But ...
            resp <- responseToEntries reqIdx
            tell [Reply clid $ Right resp]

        return $ leader & set pendingClientRequests unCommitted

    asState :: (Models.Model st req resp) -> (Proto.LogEntry req -> State st resp)
    asState m item = State.state update
      where
      update st = swap $ Models.modelStep m st $ Proto.logValue item

    responseToEntries :: Proto.LogIdx -> ProtoStateMachine st req resp resp
    responseToEntries reqIdx = do
      prev <- Map.elems <$> readLogUptoInclusive reqIdx
      m <- view model
      let (rs, _) = flip State.runState (Models.modelInit m) $ mapM (asState m) prev
      -- let st = foldl' (\s -> fst . Models.modelStep m s . Proto.logValue) (Models.modelInit m) $ Map.elems prev
      let r = last rs
      return r

handleAppendEntriesResponse sender _msg = do
    me <- view selfId
    let msg = Proto.unPeerName me ++ ": Unexpected response from " ++ show sender ++
                " got " ++ show _msg
    $(logWarnSH) msg
    void $ error $ show msg

processPeerResponseMessage :: (Show req) => Proto.PeerName -> Proto.PeerResponse -> ProtoStateMachine st req resp ()
processPeerResponseMessage name m@(Proto.VoteResult _ _)  = handleVoteResponse name m
processPeerResponseMessage name m@(Proto.AppendResult _)  = handleAppendEntriesResponse name m

processTick :: (HasCallStack, Show req, Show resp) => () -> Tick -> ProtoStateMachine st req resp ()
processTick () (Tick t) = do
    role <- use currentRole
    case role of
        Follower st -> do
            whenFollower st
        Candidate st -> do
            whenCandidate st
        Leader st -> do
            whenLeader st

    prevTickTime .= t
    return ()

    where
    whenFollower :: HasCallStack => FollowerState -> ProtoStateMachine st req resp ()
    whenFollower follower = do
        let elapsed = (t - view lastLeaderHeartbeat follower)
        timeout <- view electionTimeout
        $(logDebugSH) ("Elapsed: " , elapsed , "/" , timeout)
        if elapsed > timeout
        then do
            $(logDebugSH) "Election timeout elapsed"
            transitionToCandidate
        else return ()

    transitionToCandidate :: HasCallStack => ProtoStateMachine st req resp ()
    transitionToCandidate = do
        myName <- view selfId
        let st = CandidateState t $ Set.singleton myName
        currentRole .= (Candidate $ st)
        nextTerm <- succ <$> use currentTerm
        currentTerm .= nextTerm
        votedFor .= Just myName

        peers <- view peerNames
        myId <- view selfId
        thisTerm <- use currentTerm
        (_prevTerm, logIdx) <- getPrevLogTermIdx
        let req = Proto.RequestVoteReq thisTerm myId logIdx
        sendRequestVotes req peers
        $(logDebugSH) ("transitionToCandidate new term: " , thisTerm)
        transitiontoLeaderWithEnoughVotes myId st


    sendRequestVotes :: HasCallStack => Proto.RequestVoteReq -> Set Proto.PeerName -> ProtoStateMachine st req resp ()
    sendRequestVotes req peers =
        tell $ map (\p -> PeerRequest p (Proto.RequestVote req)) $ Set.toList peers

    whenCandidate :: CandidateState -> ProtoStateMachine st req resp ()
    whenCandidate candidate = do
        -- has the election timeout passed?
        let elapsed = t - view requestVoteSentAt candidate
        timeout <- view electionTimeout
        $(logDebugSH) ("Elapsed: " , elapsed , "/" , timeout)
        if elapsed > timeout
        then do
          $(logInfoSH) "Election timeout elapsed"
          transitionToCandidate
        else return ()

    whenLeader leader = do
        leader' <- replicatePendingEntriesToFollowers leader
        currentRole .= (Leader leader')

-- Here, we send keepalives to everything every time. This ... seems suboptimal.
replicatePendingEntriesToFollowers :: forall st req resp . (HasCallStack, Show req, Show resp)
                                   => LeaderState resp -> ProtoStateMachine st req resp (LeaderState resp)
replicatePendingEntriesToFollowers leader = do
    $(logDebugSH) ("Leader Tick")
    peers <- view peerNames
    foldM updatePeer leader peers

    where
    updatePeer :: LeaderState resp
               -> Proto.PeerName
               -> ProtoStateMachine st req resp (LeaderState resp)
    updatePeer st peer = do
        thisTerm <- use currentTerm
        myId <- view selfId
        (logTerm, logIdx) <- getPrevLogTermIdx
        let peerState = maybe (LeaderFollowerState logIdx logIdx) id $ preview (followers . ix peer) st
        let peerLastSent = view lastSent peerState

        peerPrevTerm <- maybe logTerm Proto.logTerm <$> readLogEntry peerLastSent
        toSend <- readItemsAfter peerLastSent

        let sentIdx = maybe peerLastSent fst $ Map.lookupMax $ toSend
        sendAppendEntriesRequest peer $ Proto.AppendEntriesReq thisTerm myId peerPrevTerm peerLastSent toSend
        $(logDebugSH) ("post send to", peer, "lastSent", sentIdx)
        let peerState' = peerState & set lastSent sentIdx
        return $ st & over followers (Map.insert peer peerState')

sendAppendEntriesRequest :: HasCallStack
                         => Proto.PeerName -> Proto.AppendEntriesReq req -> ProtoStateMachine st req resp ()
sendAppendEntriesRequest peer req = do
    tell [PeerRequest peer (Proto.AppendEntries req)]
