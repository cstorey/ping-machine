{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

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
, processTick
, mkRaftState
)
where

import qualified Stuff.Proto as Proto

import qualified Control.Monad.Trans.RWS.Strict as RWS
import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..))
import           Control.Monad.Reader.Class (MonadReader(..))
import           Control.Monad.Logger (WriterLoggingT, MonadLogger, logDebugSH, logWarnSH, logInfoSH)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import Control.Monad
import Data.Functor.Identity (Identity)
import Lens.Micro.Platform

import GHC.Stack
import Stuff.Types

newtype IdFor a = IdFor Int
    deriving (Show, Eq, Ord)

{-

Suspicions
We need to be able to inform the model when a response has landed. We used to do this via the
processPeerResponseMessage function. However, what we need to be able to do is:
a) Send a message to the peer,
b) Handle the response.

So, whenever the model needs to send a message to a peer, we should:
* Create a new response mvar
* Send the request + mvar to the outgoing peer bits
* push the respose mvar onto the back of a per peer dequeue

And then on each iteration, we have a per peer stm action, that:
* Takes the value of the head of the per peer mvar queue
* Drops it from the queue
* Injects the response into the model.


On the other hand, for each request, we kinda need to know what the response
is _to_, so for AppendEntries, what it's acking. So, like, maybe we need to
stash a continuation for the rest of the workflow instead? No? ...

Maybe we should reconsider what we're doing After all.

At each point in time, we should keep a note of the outgoing requests we've
made, along with the last offset .
* Receive AppendEntries response.
* Update acked offset _for that follower_
* Then derive the committed offset from a quorum of items.

So, what we need do is update the acked offset for the follower with
max(oldOffset, acked) So we need to route the response to a per follower
widget. Queue? Sub-process (ie: alternative case for STM)

---

Alternatively, use a callback type approach?

Ie: schedule an action to be run once a rpc has completed? Eg:

sendRequestVoteRpc ... $ \case
  Granted -> do
    ...
  Rejected peerTerm -> do
    ...

---

TODO:

 * Make use of appendEntriesPeriod, so that we can
 * Use more "dense" timesteps in simulation
 * Avoid random scheduling oddness.

-}

type PeerSet = Set Proto.PeerName


{-
data ProcessorMessage = Reply (IdFor Proto.ClientResponse) Proto.ClientResponse
    | PeerReply (IdFor Proto.PeerResponse) Proto.PeerResponse
    | RequestVote Proto.Term Proto.PeerName Proto.LogIdx Proto.PeerName (Receiver Proto.VoteResult)
    | RequestAppend Proto.PeerName Proto.AppendEntriesReq (Receiver Proto.AppendResult)
    deriving (Show)
-}

data ProtocolEnv = ProtocolEnv {
    _selfId :: Proto.PeerName,
    _peerNames :: PeerSet,
    _electionTimeout :: Time,
    __appendEntriesPeriod :: Time
} deriving (Show)

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
}
makeLenses ''LeaderFollowerState

data LeaderState = LeaderState {
    _followers :: Map.Map Proto.PeerName LeaderFollowerState
,   _committed :: Maybe Proto.LogIdx
,   _pendingClientRequests :: Map.Map Proto.LogIdx (IdFor Proto.ClientResponse)
}
makeLenses ''LeaderState


data RaftRole =
    Follower FollowerState
  | Candidate CandidateState
  | Leader LeaderState

data RaftState = RaftState {
    _currentRole :: RaftRole
,   _currentTerm :: Proto.Term
,   _logEntries :: Map.Map Proto.LogIdx Proto.LogEntry
,   _votedFor :: Maybe Proto.PeerName
,   _prevTickTime :: Time
,   _currentLeader :: Maybe Proto.PeerName
}
makeLenses ''RaftState


type Receiver a = a -> ProtoStateMachine ()

-- Maybe generalise Receiver to some contrafunctor?
data ProcessorMessage = Reply (IdFor Proto.ClientResponse) Proto.ClientResponse
    | PeerReply (IdFor Proto.PeerResponse) Proto.PeerResponse
    | PeerRequest Proto.PeerName Proto.PeerRequest (Receiver Proto.PeerResponse)

instance Show ProcessorMessage where
  show (Reply reqid resp) = "Reply " ++ show reqid ++ " " ++ show resp
  show (PeerReply reqid resp) = "PeerReply " ++ show reqid ++ " " ++ show resp
  show (PeerRequest name req _) = "PeerRequest " ++ show name ++ " " ++ show req ++ " #<action>"

newtype ProtoStateMachine a = ProtoStateMachine {
    runProto :: (RWS.RWST ProtocolEnv [ProcessorMessage] RaftState (WriterLoggingT Identity)) a
} deriving (Monad, Applicative, Functor, MonadState RaftState, MonadWriter [ProcessorMessage], MonadReader ProtocolEnv, MonadLogger)

mkProtocolEnv :: Proto.PeerName -> PeerSet -> Time -> Time -> ProtocolEnv
mkProtocolEnv = ProtocolEnv

newFollower :: RaftRole
newFollower = Follower $ FollowerState 0

mkRaftState :: RaftState
mkRaftState = RaftState newFollower 0 Map.empty Nothing 0 Nothing

appendToLog :: Proto.ClientRequest -> ProtoStateMachine Proto.LogIdx
appendToLog command = do
    thisTerm <- use currentTerm
    let entry = Proto.LogEntry thisTerm command
    (_, logIdx) <- getPrevLogTermIdx
    let idx = succ logIdx
    logEntries .ix idx .= entry
    return idx

processClientReqRespMessage :: Proto.ClientRequest -> IdFor Proto.ClientResponse -> ProtoStateMachine ()
processClientReqRespMessage command pendingResponse = do
    role <- use currentRole
    case role of
        Leader leader -> do
            idx <- appendToLog command
            leader' <- recordPendingClientRequest idx leader
            leader'' <- replicatePendingEntriesToFollowers leader'
            currentRole .= Leader leader''
        _ -> do
            refuseClientRequest

    where
    refuseClientRequest = do
        myLeader <- use currentLeader
        $(logDebugSH) "Not leader"
        tell [Reply pendingResponse $ Left $ Proto.NotLeader myLeader]

    recordPendingClientRequest :: HasCallStack => Monad m => Proto.LogIdx -> LeaderState -> m LeaderState
    recordPendingClientRequest idx leader = do
        return $ over pendingClientRequests (Map.insert idx pendingResponse) leader


laterTermObserved :: HasCallStack => Proto.Term -> ProtoStateMachine ()
laterTermObserved laterTerm = do
    thisTerm <- use currentTerm
    formerRole <- use currentRole
    $(logDebugSH) ("Later term observed", laterTerm, " > ", thisTerm)

    case formerRole of
        Leader leader -> whenLeader leader
        _ -> return ()

    currentTerm .= laterTerm
    votedFor .= Nothing

    void $ stepDown
    where
        whenLeader :: HasCallStack => LeaderState -> ProtoStateMachine ()
        whenLeader leader = do
            let pending = view pendingClientRequests leader
            $(logDebugSH) ("Nacking requests", pending)
            forM_ (Map.toList pending) $ \(_, clid) -> do
                -- We should really actually run a state machine here. But ...
                tell [Reply clid $ Left $ Proto.NotLeader $ Nothing]

stepDown :: HasCallStack => ProtoStateMachine FollowerState
stepDown = do
    prevTick <- use prevTickTime
    let st = FollowerState prevTick
    let role' = Follower $ st
    currentRole .= role'
    return st

getPrevLogTermIdx :: HasCallStack => ProtoStateMachine (Proto.Term, Proto.LogIdx)
getPrevLogTermIdx = do
    myLog <- use logEntries
    return $ maybe (0, 0) (\(idx, it) -> (Proto.logTerm it, idx)) $ Map.lookupMax myLog

getMajority :: HasCallStack => ProtoStateMachine Int
getMajority = do
    memberCount <- succ . length <$> view peerNames
    return $ succ (memberCount `div` 2)

processPeerRequestMessage :: HasCallStack => Proto.PeerRequest -> IdFor Proto.PeerResponse -> ProtoStateMachine ()
processPeerRequestMessage (Proto.RequestVote req) sender = do
    -- let x = (RequestVoteReq candidateTerm candidateName candidateIdx);
    thisTerm <- use currentTerm
    vote <- use votedFor
    let candidateTerm = Proto.rvTerm req
    (_prevTerm, logIdx) <- getPrevLogTermIdx
    case () of
        _ | candidateTerm < thisTerm -> do
            $(logDebugSH) (candidateTerm, " < ", thisTerm)
            refuseVote thisTerm
        _ | candidateTerm > thisTerm -> do
            laterTermObserved candidateTerm
            $(logDebugSH) ("Granting vote")
            grantVote candidateTerm
        _ | Maybe.isNothing vote && Proto.rvHead req >= logIdx -> do
            $(logDebugSH) ("Granting vote")
            grantVote candidateTerm
        _ -> do
            $(logDebugSH) ("Already voted for", vote)
            refuseVote thisTerm

    return ()

    where
        refuseVote :: HasCallStack => Proto.Term -> ProtoStateMachine ()
        refuseVote thisTerm = tell [PeerReply sender $ Proto.VoteResult thisTerm False]
        grantVote :: HasCallStack => Proto.Term -> ProtoStateMachine ()
        grantVote thisTerm = do
            votedFor .= Just (Proto.rvName req)
            tell [PeerReply sender $ Proto.VoteResult thisTerm True]

processPeerRequestMessage
    _msg@(Proto.AppendEntries (Proto.AppendEntriesReq leaderTerm leaderName assumedHeadTerm assumedHeadIdx newEntries)) reqId = do
    -- prevTick <- prevTickTime <$> get
    $(logDebugSH) ("<- Append Entries", _msg)

    thisTerm <- use currentTerm
    if leaderTerm < thisTerm
    then do
        return ()
        $(logDebugSH) ("Refuse appendentries", leaderTerm, " < ", thisTerm)
        refuseAppendEntries thisTerm
    else
        if leaderTerm > thisTerm
        then laterTermObserved leaderTerm
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
        refuseAppendEntries :: HasCallStack => Proto.Term -> ProtoStateMachine ()
        refuseAppendEntries thisTerm = tell [PeerReply reqId $ Proto.AppendResult $ Proto.AppendEntriesResponse thisTerm False]
        ackAppendEntries :: HasCallStack => Proto.Term -> ProtoStateMachine ()
        ackAppendEntries thisTerm = tell [PeerReply reqId $ Proto.AppendResult $ Proto.AppendEntriesResponse thisTerm True]

        whenFollower thisTerm follower = do
            prevTick <- use prevTickTime
            let follower' = set lastLeaderHeartbeat prevTick follower
            currentRole .= Follower follower'

            (_prevTerm, logHeadIdx) <- getPrevLogTermIdx
            entries <- use logEntries

            let myEntry = Map.lookup assumedHeadIdx entries
            case Proto.logTerm <$> myEntry of
                Just n | n /= assumedHeadTerm -> do
                    $(logDebugSH) ("Refusing as prev item was", myEntry, "wanted term", assumedHeadTerm)
                    refuseAppendEntries thisTerm
                -- We need to refuse here iff it's ahead of our log
                Nothing | assumedHeadIdx > logHeadIdx -> do
                    $(logDebugSH) ("Refusing as prevIdx index", assumedHeadIdx, "ahead of our", logHeadIdx)
                    refuseAppendEntries thisTerm
                _ -> do
                    let (entries', _)  = Map.split (succ assumedHeadIdx) entries
                    let entries'' = Map.union entries' newEntries
                    logEntries .= entries''
                    currentLeader .= Just leaderName
                    ackAppendEntries thisTerm

handleVoteResponse :: HasCallStack => Proto.RequestVoteReq -> Proto.PeerName -> Proto.PeerResponse -> ProtoStateMachine ()
handleVoteResponse req _sender _msg@(Proto.VoteResult peerTerm granted) = do
    $(logDebugSH) ("handleVoteResponse to", req, _msg)
    role <- use currentRole
    myTerm <- use currentTerm

    case role of
        _ | peerTerm > myTerm -> laterTermObserved peerTerm
        _ | peerTerm < myTerm -> $(logDebugSH) ("Ignoring vote for previous term")
        Candidate st -> do
            whenCandidate myTerm st
        _ -> do
            $(logDebugSH) ("vote recieved when non candidate?")
    where
    whenCandidate myTerm candidate = do
        if granted
        then do
            let newCandidate = over votesForMe (Set.insert _sender) candidate
            let currentVotes = view votesForMe newCandidate

            neededVotes <- getMajority

            $(logDebugSH) (
                "In term: " , myTerm , " Vote ack for term: " , peerTerm ,
                " needed: " , neededVotes , " have: " , currentVotes)

            let newRole = if length currentVotes >= neededVotes
                then Leader $ LeaderState Map.empty Nothing Map.empty
                else Candidate newCandidate

            currentRole .= newRole
        else
            return ()
handleVoteResponse req sender _msg = do
    me <- view selfId
    let msg =
            (Proto.unPeerName me , ": Unexpected response from " , sender ,
            " to vote request " , req , " resp " , _msg)
    -- error msg
    -- Should at least be a warning, or something.
    $(logWarnSH) msg


handleAppendEntriesResponse :: HasCallStack => Proto.AppendEntriesReq -> Proto.PeerName -> Proto.PeerResponse -> ProtoStateMachine ()
handleAppendEntriesResponse _ sender _msg@(Proto.AppendResult aer) = do
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
        -- FIXME: This is wrong, as it'll produce incorrect results when we
        -- have multiple overlaping requests. We need to correlate these with the actually sent id.
        -- Then again, this is invoked via callback, so should be trivial.

        case preview (followers . ix sender . lastSent) leader of
            Just sentIdx -> do
                if Proto.aerSucceeded aer
                then do
                    -- let followerPrevIdx' = Map.insert sender sentIdx $ view followerPrevIdx leader
                    committedIdx <- findCommittedIndex leader

                    $(logDebugSH) ("committed index should be", committedIdx)

                    return $ leader
                        & set (followers . ix sender . prevIdx) sentIdx
                        & set committed committedIdx

                else do
                    let toTry = Proto.LogIdx . (`div` 2) . Proto.unLogIdx $ sentIdx
                    $(logDebugSH) ("Retry peer " , sender , " at : " , toTry)
                    return $ leader & set (followers . ix sender . prevIdx) toTry
            _ -> do
                  $(logDebugSH) ("Response to an unsent message? from " , sender)
                  return leader

    findCommittedIndex :: HasCallStack => LeaderState -> ProtoStateMachine (Maybe Proto.LogIdx)
    findCommittedIndex st = do
        majority <- getMajority
        -- Find items acked by a ajority of servers.
        -- So first derive the acked idxes in descending order
        (_, myIdx ) <- getPrevLogTermIdx
        let allIndexes = myIdx : toListOf (followers . each . prevIdx) st
        let known = List.sortOn (0 -) $ allIndexes
        $(logDebugSH) ("Known follower indexes: " , known)
        return $ Maybe.listToMaybe $ List.drop (pred majority) known

    ackPendingClientResponses :: HasCallStack => LeaderState -> Proto.LogIdx -> ProtoStateMachine LeaderState
    ackPendingClientResponses leader idx = do
        let pending = view pendingClientRequests leader
        let (canRespond, unCommitted) = Map.spanAntitone (<= idx) pending
        $(logDebugSH) ("committed index: " , idx , " pending: " , canRespond)

        $(logDebugSH) ("Responding to requests: " , canRespond)
        forM_ (Map.toList canRespond) $ \(reqIdx, clid) -> do
            -- We should really actually run a state machine here. But ...
            tell [Reply clid $ Right $ Proto.Bong $ show reqIdx]

        return $ leader & set pendingClientRequests unCommitted


handleAppendEntriesResponse req sender _msg = do
    me <- view selfId
    let msg = Proto.unPeerName me ++ ": Unexpected response from " ++ show sender ++
                " to append entries request:" ++ show req ++
                " got " ++ show _msg
    -- error msg
    -- Should at least be a warning, or something.
    $(logDebugSH) msg

processTick :: HasCallStack => () -> Tick -> ProtoStateMachine ()
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
    whenFollower :: HasCallStack => FollowerState -> ProtoStateMachine ()
    whenFollower follower = do
        let elapsed = (t - view lastLeaderHeartbeat follower)
        timeout <- view electionTimeout
        $(logDebugSH) ("Elapsed: " , elapsed , "/" , timeout)
        if elapsed > timeout
        then do
            $(logDebugSH) "Election timeout elapsed"
            transitionToCandidate
        else return ()

    transitionToCandidate :: HasCallStack => ProtoStateMachine ()
    transitionToCandidate = do
        myName <- view selfId
        currentRole .= (Candidate $ CandidateState t $ Set.singleton myName)
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


    sendRequestVotes :: HasCallStack => Proto.RequestVoteReq -> Set Proto.PeerName -> ProtoStateMachine ()
    sendRequestVotes req peers =
        tell $ map (\p -> PeerRequest p (Proto.RequestVote req) $ handleVoteResponse req p) $ Set.toList peers

    whenCandidate :: CandidateState -> ProtoStateMachine ()
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


replicatePendingEntriesToFollowers :: HasCallStack => LeaderState -> ProtoStateMachine LeaderState
replicatePendingEntriesToFollowers leader = do
    $(logDebugSH) ("Leader Tick")
    peers <- view peerNames
    foldM updatePeer leader peers

    where
    updatePeer :: LeaderState
               -> Proto.PeerName
               -> ProtoStateMachine LeaderState

    updatePeer st peer = do
        thisTerm <- use currentTerm
        myId <- view selfId
        (logTerm, logIdx) <- getPrevLogTermIdx
        let peerLastSent = maybe logIdx id $ preview (followers . ix peer . lastSent) st
        entries <- use logEntries
        let peerPrevTerm = maybe logTerm Proto.logTerm $ Map.lookup peerLastSent entries
        -- Send everything _after_ their previous index
        let toSend = snd $ Map.split peerLastSent entries
        let peerPrevIdx = maybe logIdx id $ preview (followers . ix peer . prevIdx) st
        sendAppendEntriesRequest peer $ Proto.AppendEntriesReq thisTerm myId peerPrevTerm peerPrevIdx toSend
        let lastSent' = maybe peerPrevIdx fst $ Map.lookupMax toSend
        return $ st & set (followers . ix peer . lastSent) lastSent'


sendAppendEntriesRequest :: HasCallStack => Proto.PeerName -> Proto.AppendEntriesReq -> ProtoStateMachine ()
sendAppendEntriesRequest peer req = do
    tell [PeerRequest peer (Proto.AppendEntries req) $ handleAppendEntriesResponse req peer]
