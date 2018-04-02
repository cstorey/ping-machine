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
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Debug.Trace as Trace
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import Control.Monad
import Lens.Micro.Platform

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
    runProto :: RWS.RWS ProtocolEnv [ProcessorMessage] RaftState a
} deriving (Monad, Applicative, Functor, MonadState RaftState, MonadWriter [ProcessorMessage], MonadReader ProtocolEnv)

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
        Trace.trace "Not leader" $ return ()
        tell [Reply pendingResponse $ Left $ Proto.NotLeader myLeader]

    recordPendingClientRequest :: Monad m => Proto.LogIdx -> LeaderState -> m LeaderState
    recordPendingClientRequest idx leader = do
        return $ over pendingClientRequests (Map.insert idx pendingResponse) leader


laterTermObserved :: Proto.Term -> ProtoStateMachine ()
laterTermObserved laterTerm = do
    thisTerm <- use currentTerm
    formerRole <- use currentRole
    Trace.trace ("Later term observed: " ++ show laterTerm ++ " > " ++ show thisTerm) $ return ()

    case formerRole of
        Leader leader -> whenLeader leader
        _ -> return ()

    currentTerm .= laterTerm
    votedFor .= Nothing

    stepDown
    where
        whenLeader :: LeaderState -> ProtoStateMachine ()
        whenLeader leader = do
            let pending = view pendingClientRequests leader
            Trace.trace ("Nacking requests: " ++ show pending) $ return ()
            forM_ (Map.toList pending) $ \(_, clid) -> do
                -- We should really actually run a state machine here. But ...
                tell [Reply clid $ Left $ Proto.NotLeader $ Nothing]

stepDown :: ProtoStateMachine ()
stepDown = do
    prevTick <- use prevTickTime
    let role' = Follower $ FollowerState prevTick
    currentRole .= role'

getPrevLogTermIdx :: ProtoStateMachine (Proto.Term, Proto.LogIdx)
getPrevLogTermIdx = do
    myLog <- use logEntries
    return $ maybe (0, 0) (\(idx, it) -> (Proto.logTerm it, idx)) $ Map.lookupMax myLog

getMajority :: ProtoStateMachine Int
getMajority = do
    memberCount <- succ . length <$> view peerNames
    return $ succ (memberCount `div` 2)

processPeerRequestMessage :: Proto.PeerRequest -> IdFor Proto.PeerResponse -> ProtoStateMachine ()
processPeerRequestMessage (Proto.RequestVote candidateTerm candidateName candidateIdx) sender = do
    thisTerm <- use currentTerm
    vote <- use votedFor
    (_prevTerm, logIdx) <- getPrevLogTermIdx
    case () of
        _ | candidateTerm < thisTerm -> do
            Trace.trace (show candidateTerm ++ " < " ++ show thisTerm) $ return ()
            refuseVote thisTerm
        _ | candidateTerm > thisTerm -> do
            laterTermObserved candidateTerm
            Trace.trace ("Granting vote") $ grantVote candidateTerm
            -- Reply No?
        _ | Maybe.isNothing vote && candidateIdx >= logIdx -> do
            Trace.trace ("Granting vote") $ return ()
            grantVote candidateTerm
            -- Already
        _ -> do
            Trace.trace ("Already voted for " ++ show vote) $ return ()
            refuseVote thisTerm

    return ()

    where
        refuseVote :: Proto.Term -> ProtoStateMachine ()
        refuseVote thisTerm = tell [PeerReply sender $ Proto.VoteResult thisTerm False]
        grantVote :: Proto.Term -> ProtoStateMachine ()
        grantVote thisTerm = do
            votedFor .= Just candidateName
            tell [PeerReply sender $ Proto.VoteResult thisTerm True]

processPeerRequestMessage
    _msg@(Proto.AppendEntries (Proto.AppendEntriesReq leaderTerm leaderName assumedHeadTerm assumedHeadIdx newEntries)) reqId = do
    -- prevTick <- prevTickTime <$> get
    Trace.trace ("<- Append Entries: " ++ show _msg) $ return ()

    thisTerm <- use currentTerm
    if leaderTerm < thisTerm
    then do
        return ()
        Trace.trace (show leaderTerm ++ " < " ++ show thisTerm) $ return ()
        Trace.trace ("Refuse appendentries") $ return ()
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
                    stepDown
                Leader _st -> do
                    error ("appendEntries recieved when leader? " ++ show _msg)

    where
        refuseAppendEntries :: Proto.Term -> ProtoStateMachine ()
        refuseAppendEntries thisTerm = tell [PeerReply reqId $ Proto.AppendResult $ Proto.AppendEntriesResponse thisTerm False]
        ackAppendEntries :: Proto.Term -> ProtoStateMachine ()
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
                    Trace.trace ("Refusing as prev item was: " ++ show myEntry ++ " wanted term " ++ show assumedHeadTerm) $
                        refuseAppendEntries thisTerm
                -- We need to refuse here iff it's ahead of our log
                Nothing | assumedHeadIdx > logHeadIdx -> do
                    Trace.trace ("Refusing as prevIdx index: " ++ show assumedHeadIdx ++ " ahead of our " ++ show logHeadIdx) $
                        refuseAppendEntries thisTerm
                _ -> do
                    let (entries', _)  = Map.split (succ assumedHeadIdx) entries
                    let entries'' = Map.union entries' newEntries
                    logEntries .= entries''
                    currentLeader .= Just leaderName
                    ackAppendEntries thisTerm

processPeerResponseMessage :: Proto.PeerName -> Proto.PeerResponse -> ProtoStateMachine ()
processPeerResponseMessage _sender _msg@(Proto.VoteResult peerTerm granted) = do
    Trace.trace ("processPeerResponseMessage: " ++ show _msg) $ return ()
    role <- use currentRole
    myTerm <- use currentTerm

    case role of
        _ | peerTerm > myTerm -> laterTermObserved peerTerm
        _ | peerTerm < myTerm -> Trace.trace ("Ignoring vote for previous term") $ return ()
        Candidate st -> do
            whenCandidate myTerm st
        _ -> do
            Trace.trace ("vote recieved when non candidate?") $ return ()
    where
    whenCandidate myTerm candidate = do
        if granted
        then do
            let newCandidate = over votesForMe (Set.insert _sender) candidate
            let currentVotes = view votesForMe newCandidate

            neededVotes <- getMajority

            Trace.trace ("In term: " ++ show myTerm ++
                " Vote ack for term: " ++ show peerTerm ++
                " needed: " ++ show neededVotes ++
                " have: " ++ show currentVotes
                ) $ return ()

            let newRole = if length currentVotes >= neededVotes
                then Leader $ LeaderState Map.empty Nothing Map.empty
                else Candidate newCandidate

            currentRole .= newRole
        else
            return ()

processPeerResponseMessage sender _msg@(Proto.AppendResult aer) = do
    Trace.trace ("processPeerResponseMessage: " ++ show _msg) $ return ()
    role <- use currentRole
    case role of
        Leader leader -> do
            leader' <- whenLeader leader
            leader'' <- maybe (return leader') (ackPendingClientResponses leader' ) $ view committed leader'
            currentRole .= (Leader $ leader'')
        _st -> do
            Trace.trace ("append response recieved when non leader? " ++ show _msg) $ return ()
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

                    Trace.trace ("committed index should be: " ++ show committedIdx) $ return ()

                    return $ leader
                        & set (followers . ix sender . prevIdx) sentIdx
                        & set committed committedIdx

                else do
                    let toTry = Proto.LogIdx . (`div` 2) . Proto.unLogIdx $ sentIdx
                    Trace.trace ("Retry peer " ++ show sender ++ " at : " ++ show toTry) $ return ()
                    return $ leader & set (followers . ix sender . prevIdx) toTry
            _ -> Trace.trace ("Response to an unsent message? from " ++ show sender) $ return leader

    findCommittedIndex :: LeaderState -> ProtoStateMachine (Maybe Proto.LogIdx)
    findCommittedIndex st = do
        majority <- getMajority
        -- Find items acked by a ajority of servers.
        -- So first derive the acked idxes in descending order
        (_, myIdx ) <- getPrevLogTermIdx
        let allIndexes = myIdx : toListOf (followers . each . prevIdx) st
        let known = List.sortOn (0 -) $ allIndexes
        Trace.trace ("Known follower indexes: " ++ show known) $ return ()
        return $ Maybe.listToMaybe $ List.drop (pred majority) known

    ackPendingClientResponses :: LeaderState -> Proto.LogIdx -> ProtoStateMachine LeaderState
    ackPendingClientResponses leader idx = do
        let pending = view pendingClientRequests leader
        let (canRespond, unCommitted) = Map.spanAntitone (<= idx) pending
        Trace.trace ("committed index: " ++ show idx ++ " pending: " ++ show canRespond) $ return ()

        Trace.trace ("Responding to requests: " ++ show canRespond) $ return ()
        forM_ (Map.toList canRespond) $ \(reqIdx, clid) -> do
            -- We should really actually run a state machine here. But ...
            tell [Reply clid $ Right $ Proto.Bong $ show reqIdx]

        return $ leader & set pendingClientRequests unCommitted



processTick :: () -> Tick -> ProtoStateMachine ()
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
    whenFollower :: FollowerState -> ProtoStateMachine ()
    whenFollower follower = do
        let elapsed = (t - view lastLeaderHeartbeat follower)
        timeout <- view electionTimeout
        Trace.trace ("Elapsed: " ++ show elapsed ++ "/" ++ show timeout) $ return ()
        if elapsed > timeout
        then Trace.trace "Election timeout elapsed" $ transitionToCandidate
        else return ()

    transitionToCandidate :: ProtoStateMachine ()
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
        let req = Proto.RequestVote thisTerm myId logIdx
        tell $ map (\p -> PeerRequest p req $ processPeerResponseMessage p) $ Set.toList peers
        Trace.trace ("transitionToCandidate new term: " ++ show thisTerm) $ return ()

    whenCandidate :: CandidateState -> ProtoStateMachine ()
    whenCandidate candidate = do
        -- has the election timeout passed?
        let elapsed = t - view requestVoteSentAt candidate
        timeout <- view electionTimeout
        Trace.trace ("Elapsed: " ++ show elapsed ++ "/" ++ show timeout) $ return ()
        if elapsed > timeout
        then Trace.trace "Election timeout elapsed" $ transitionToCandidate
        else return ()

    whenLeader leader = do
        leader' <- replicatePendingEntriesToFollowers leader
        currentRole .= (Leader leader')


replicatePendingEntriesToFollowers :: LeaderState -> ProtoStateMachine LeaderState
replicatePendingEntriesToFollowers leader = do
    Trace.trace ("Leader Tick") $ return ()
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
        sendPeerRequest peer $ Proto.AppendEntries $ Proto.AppendEntriesReq thisTerm myId peerPrevTerm peerPrevIdx toSend
        let lastSent' = maybe peerPrevIdx fst $ Map.lookupMax toSend
        return $ st & set (followers . ix peer . lastSent) lastSent'


sendPeerRequest :: Proto.PeerName -> Proto.PeerRequest -> ProtoStateMachine ()
sendPeerRequest peer req = tell [PeerRequest peer req $ processPeerResponseMessage peer]
