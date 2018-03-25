{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.RaftModel
( ProcessorMessage(..)
, ProtoStateMachine(..)
, RaftState
, ProtocolEnv(..)
, IdFor(..)
, processClientReqRespMessage
, processPeerRequestMessage
, processTick
, mkRaftState
)
where

import qualified Lib

import qualified Control.Monad.Trans.RWS.Strict as RWS
import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..), modify)
import           Control.Monad.Reader.Class (MonadReader(..), asks)
import qualified Data.Map as Map
import qualified Debug.Trace as Trace
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import Control.Monad

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

-}

type Receiver a = a -> ProtoStateMachine ()

-- Maybe generalise Receiver to some contrafunctor?
data ProcessorMessage = Reply (IdFor Lib.ClientResponse) Lib.ClientResponse
    | PeerReply (IdFor Lib.PeerResponse) Lib.PeerResponse
    | PeerRequest Lib.PeerName Lib.PeerRequest (Receiver Lib.PeerResponse)

instance Show ProcessorMessage where
  show (Reply reqid resp) = "Reply " ++ show reqid ++ " " ++ show resp
  show (PeerReply reqid resp) = "PeerReply " ++ show reqid ++ " " ++ show resp
  show (PeerRequest name req _) = "PeerRequest " ++ show name ++ " " ++ show req ++ " #<action>"

{-
data ProcessorMessage = Reply (IdFor Lib.ClientResponse) Lib.ClientResponse
    | PeerReply (IdFor Lib.PeerResponse) Lib.PeerResponse
    | RequestVote Lib.Term Lib.PeerName Lib.LogIdx Lib.PeerName (Receiver Lib.VoteResult)
    | RequestAppend Lib.PeerName Lib.AppendEntriesReq (Receiver Lib.AppendResult)
    deriving (Show)
-}

data ProtocolEnv = ProtocolEnv {
    selfId :: Lib.PeerName,
    peerNames :: [Lib.PeerName]
} deriving (Show)

data FollowerState = FollowerState {
    lastLeaderHeartbeat :: Time
} deriving (Show)

data CandidateState = CandidateState {
    requestVoteSentAt :: Time
,   votesForMe :: [Lib.PeerName]
} deriving (Show)

data LeaderState = LeaderState {
    followerPrevIdx :: Map.Map Lib.PeerName Lib.LogIdx
,   followerLastSent :: Map.Map Lib.PeerName Lib.LogIdx
,   committed :: Maybe Lib.LogIdx
,   pendingClientRequests :: Map.Map Lib.LogIdx (IdFor Lib.ClientResponse)
} deriving (Show)

data RaftRole =
    Follower FollowerState
  | Candidate CandidateState
  | Leader LeaderState
    deriving (Show)

data RaftState = RaftState {
    currentRole :: RaftRole
,   currentTerm :: Lib.Term
,   logEntries :: Map.Map Lib.LogIdx Lib.LogEntry
,   votedFor :: Maybe Lib.PeerName
,   prevTickTime :: Time
,   currentLeader :: Maybe Lib.PeerName
} deriving (Show)

newtype ProtoStateMachine a = ProtoStateMachine {
    runProto :: RWS.RWS ProtocolEnv [ProcessorMessage] RaftState a
} deriving (Monad, Applicative, Functor, MonadState RaftState, MonadWriter [ProcessorMessage], MonadReader ProtocolEnv)

oneSecond :: Time
oneSecond = 1

timeout :: Time
timeout = oneSecond * 3

newFollower :: RaftRole
newFollower = Follower $ FollowerState 0

mkRaftState :: RaftState
mkRaftState = RaftState newFollower 0 Map.empty Nothing 0 Nothing

appendToLog :: Lib.ClientRequest -> ProtoStateMachine Lib.LogIdx
appendToLog command = do
    thisTerm <- currentTerm <$> get
    let entry = Lib.LogEntry thisTerm command
    (_, prevIdx) <- getPrevLogTermIdx
    let idx = succ prevIdx
    modify $ \st -> st {
        logEntries = Map.insert idx entry $ logEntries st
    }
    return idx

processClientReqRespMessage :: Lib.ClientRequest -> IdFor Lib.ClientResponse -> ProtoStateMachine ()
processClientReqRespMessage command pendingResponse = do
    role <- currentRole <$> get
    case role of
        Leader leader -> do
            idx <- appendToLog command
            leader' <- recordPendingClientRequest idx leader
            leader'' <- replicatePendingEntriesToFollowers leader'
            modify $ \st -> st { currentRole = Leader leader'' }
        _ -> do
            refuseClientRequest

    where
    refuseClientRequest = do
        myLeader <- currentLeader <$> get
        Trace.trace "Not leader" $ return ()
        tell [Reply pendingResponse $ Left $ Lib.NotLeader myLeader]

    recordPendingClientRequest idx leader = do
        return $ leader { pendingClientRequests = Map.insert idx pendingResponse $ pendingClientRequests leader}


laterTermObserved :: Lib.Term -> ProtoStateMachine ()
laterTermObserved laterTerm = do
    thisTerm <- currentTerm <$> get
    formerRole <- currentRole <$> get
    Trace.trace ("Later term observed: " ++ show laterTerm ++ " > " ++ show thisTerm) $ return ()
    prevTick <- prevTickTime <$> get

    case formerRole of
        Leader leader -> whenLeader leader
        _ -> return ()

    modify $ \st -> st {
        currentRole = Follower $ FollowerState {
            lastLeaderHeartbeat = prevTick
        },
        currentTerm = laterTerm,
        votedFor = Nothing
    }

    where
    whenLeader :: LeaderState -> ProtoStateMachine ()
    whenLeader leader = do
        let pending = pendingClientRequests leader
        Trace.trace ("Nacking requests: " ++ show pending) $ return ()
        forM_ (Map.toList pending) $ \(_, clid) -> do
            -- We should really actually run a state machine here. But ...
            tell [Reply clid $ Left $ Lib.NotLeader $ Nothing]

getPrevLogTermIdx :: ProtoStateMachine (Lib.Term, Lib.LogIdx)
getPrevLogTermIdx = do
    myLog <- logEntries <$> get
    return $ maybe (0, 0) (\(idx, it) -> (Lib.logTerm it, idx)) $ Map.lookupMax myLog



getMajority :: ProtoStateMachine Int
getMajority = do
    memberCount <- succ . length <$> asks peerNames
    return $ succ (memberCount `div` 2)

processPeerRequestMessage :: Lib.PeerRequest -> IdFor Lib.PeerResponse -> ProtoStateMachine ()
processPeerRequestMessage (Lib.RequestVote candidateTerm candidateName candidateIdx) sender = do
    thisTerm <- currentTerm <$> get
    vote <- votedFor <$> get
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
    {-
    tell [PeerReply sender $ Lib.ThatsNiceDear n]
    -}

    where
        refuseVote :: Lib.Term -> ProtoStateMachine ()
        refuseVote thisTerm = tell [PeerReply sender $ Lib.VoteResult thisTerm False]
        grantVote :: Lib.Term -> ProtoStateMachine ()
        grantVote thisTerm = do
            modify $ \st -> st { votedFor = Just candidateName }
            tell [PeerReply sender $ Lib.VoteResult thisTerm True]

processPeerRequestMessage
    _msg@(Lib.AppendEntries (Lib.AppendEntriesReq leaderTerm leaderName prevTerm prevIdx newEntries)) reqId = do
    -- prevTick <- prevTickTime <$> get
    Trace.trace ("Append Entries: " ++ show _msg) $ return ()

    thisTerm <- currentTerm <$> get
    -- myLog <- logEntries <$> get
    -- let logIdx = maybe (0, 0) fst $ Map.lookupMax myLog
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
            role <- currentRole <$> get
            case role of
                Follower follower -> do
                    whenFollower thisTerm follower
                Candidate _st -> do
                    error ("appendEntries recieved when leader? " ++ show _msg)
                Leader _st -> do
                    error ("appendEntries recieved when leader? " ++ show _msg)

    where
        refuseAppendEntries :: Lib.Term -> ProtoStateMachine ()
        refuseAppendEntries thisTerm = tell [PeerReply reqId $ Lib.AppendResult $ Lib.AppendEntriesResponse thisTerm False]
        ackAppendEntries :: Lib.Term -> ProtoStateMachine ()
        ackAppendEntries thisTerm = tell [PeerReply reqId $ Lib.AppendResult $ Lib.AppendEntriesResponse thisTerm True]

        whenFollower thisTerm follower = do

            prevTick <- prevTickTime <$> get
            let follower' = follower { lastLeaderHeartbeat = prevTick }
            modify $ \st -> st { currentRole = Follower follower' }

            (_prevTerm, logHeadIdx) <- getPrevLogTermIdx
            entries <- logEntries <$> get

            let myEntry = Map.lookup prevIdx entries
            case Lib.logTerm <$> myEntry of
                Just n | n /= prevTerm -> do
                    Trace.trace ("Refusing as prev item was: " ++ show myEntry ++ " wanted term " ++ show prevTerm) $
                        refuseAppendEntries thisTerm
                -- We need to refuse here iff it's ahead of our log
                Nothing | prevIdx > logHeadIdx -> do
                    Trace.trace ("Refusing as prevIdx index: " ++ show prevIdx ++ " ahead of our " ++ show logHeadIdx) $
                        refuseAppendEntries thisTerm
                _ -> do
                    let idx = succ prevIdx
                    let (entries', _)  = Map.split idx entries
                    let entries'' = Map.union entries' newEntries
                    modify $ \st -> st {
                        logEntries = entries'',
                        currentLeader = Just leaderName
                    }
                    ackAppendEntries thisTerm

processPeerResponseMessage :: Lib.PeerName -> Lib.PeerResponse -> ProtoStateMachine ()
processPeerResponseMessage _sender _msg@(Lib.VoteResult peerTerm granted) = do
    Trace.trace ("processPeerResponseMessage: " ++ show _msg) $ return ()
    role <- currentRole <$> get
    myTerm <- currentTerm <$> get

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
            let newCandidate = candidate {
                votesForMe = _sender : votesForMe candidate
            }

            let currentVotes = votesForMe newCandidate

            neededVotes <- getMajority

            Trace.trace ("In term: " ++ show myTerm ++
                " Vote ack for term: " ++ show peerTerm ++
                " needed: " ++ show neededVotes ++
                " have: " ++ show currentVotes
                ) $ return ()

            let newRole = if length currentVotes >= neededVotes
                then Leader $ LeaderState Map.empty Map.empty Nothing Map.empty
                else Candidate newCandidate

            modify $ \st -> st {
                currentRole = newRole
            }
        else
            return ()

processPeerResponseMessage sender _msg@(Lib.AppendResult aer) = do
    Trace.trace ("processPeerResponseMessage: " ++ show _msg) $ return ()
    role <- currentRole <$> get
    case role of
        Leader leader -> do
            leader' <- whenLeader leader
            leader'' <- maybe (return leader') (ackPendingClientResponses leader' ) $ committed leader'

            modify $ \st -> st { currentRole = Leader $ leader'' }
        _st -> do
            Trace.trace ("append response recieved when non leader? " ++ show _msg) $ return ()
    where
    whenLeader leader = do
        let lastSent = followerLastSent leader
        case Map.lookup sender lastSent of
            Just sentIdx -> do
                if Lib.aerSucceeded aer
                then do
                    let followerPrevIdx' = Map.insert sender sentIdx $ followerPrevIdx leader
                    committedIdx <- findCommittedIndex followerPrevIdx'

                    Trace.trace ("committed index should be: " ++ show committedIdx) $ return ()

                    return leader {
                        followerPrevIdx = followerPrevIdx'
                    ,   followerLastSent = Map.delete sender lastSent
                    ,   committed = committedIdx
                    }

                else do
                    let toTry = Lib.LogIdx . (`div` 2) . Lib.unLogIdx $ sentIdx
                    let lastSent' = Map.insert sender toTry $ followerLastSent leader
                    Trace.trace ("Retry peer " ++ show sender ++ " at : " ++ show toTry) $ return ()
                    return leader {
                        followerLastSent = lastSent'
                    }
            _ -> Trace.trace ("Response to an unsent message?") $ return leader

    findCommittedIndex followerPrevIdx' = do
        majority <- getMajority
        -- Find items acked by a ajority of servers.
        -- So first derive the acked idxes in descending order
        (_, myIdx ) <- getPrevLogTermIdx
        let known = List.sortOn (0 -) $ myIdx : Map.elems followerPrevIdx'
        Trace.trace ("Known follower indexes: " ++ show known) $ return ()
        return $ Maybe.listToMaybe $ List.drop (pred majority) known

    ackPendingClientResponses :: LeaderState -> Lib.LogIdx -> ProtoStateMachine LeaderState
    ackPendingClientResponses leader idx = do
        let pending = pendingClientRequests leader
        let (canRespond, unCommitted) = Map.spanAntitone (<= idx) pending
        Trace.trace ("committed index: " ++ show idx ++ " pending: " ++ show canRespond) $ return ()

        Trace.trace ("Responding to requests: " ++ show canRespond) $ return ()
        forM_ (Map.toList canRespond) $ \(reqIdx, clid) -> do
            -- We should really actually run a state machine here. But ...
            tell [Reply clid $ Right $ Lib.Bong $ show reqIdx]

        return $ leader { pendingClientRequests = unCommitted }



processTick :: () -> Tick -> ProtoStateMachine ()
processTick () (Tick t) = do
    role <- currentRole <$> get
    case role of
        Follower st -> do
            whenFollower st
        Candidate st -> do
            whenCandidate st
        Leader st -> do
            whenLeader st

    -- toSend <- pending <$> get
    -- modify $ \st -> st { pending = [] }
    -- tell toSend
    modify $ \st -> st { prevTickTime = t }
    return ()

    where
    whenFollower follower = do
        let elapsed = (t - lastLeaderHeartbeat follower)
        Trace.trace ("Elapsed: " ++ show elapsed ) $ return ()
        if elapsed > timeout
        then Trace.trace "Election timeout elapsed" $ transitionToCandidate
        else return ()

    transitionToCandidate = do
        myName <- asks selfId
        modify $ \st -> st {
            currentRole = Candidate $ CandidateState t [myName],
            currentTerm = currentTerm st + 1,
            votedFor = Just myName
        }
        peers <- asks peerNames
        myId <- asks selfId
        thisTerm <- currentTerm <$> get
        (_prevTerm, prevIdx) <- getPrevLogTermIdx
        let req = Lib.RequestVote thisTerm myId prevIdx
        tell $ map (\p -> PeerRequest p req $ processPeerResponseMessage p) peers
        Trace.trace ("transitionToCandidate new term: " ++ show thisTerm) $ return ()

    whenCandidate candidate = do
        -- has the election timeout passed?
        let elapsed = t - requestVoteSentAt candidate
        Trace.trace ("Elapsed: " ++ show elapsed ) $ return ()
        if elapsed > timeout
        then Trace.trace "Election timeout elapsed" $ transitionToCandidate
        else return ()

    whenLeader leader = do
        leader' <- replicatePendingEntriesToFollowers leader
        modify $ \st -> st {
            currentRole = Leader $ leader'
        }


replicatePendingEntriesToFollowers :: LeaderState -> ProtoStateMachine LeaderState
replicatePendingEntriesToFollowers leader = do
    Trace.trace ("Leader Tick") $ return ()
    peers <- asks peerNames
    let lastSent = followerLastSent leader
    let peerPrevIxes = followerPrevIdx leader
    peerSentIxes <- foldM (updatePeer peerPrevIxes) lastSent peers

    return $ leader { followerLastSent = peerSentIxes }

    where
    updatePeer :: Map.Map Lib.PeerName Lib.LogIdx
               -> Map.Map Lib.PeerName Lib.LogIdx
               -> Lib.PeerName
               -> ProtoStateMachine (Map.Map Lib.PeerName Lib.LogIdx)

    updatePeer peerPrevIxes lastSentTo peer = do
        thisTerm <- currentTerm <$> get
        myId <- asks selfId
        (prevTerm, prevIdx) <- getPrevLogTermIdx
        let peerPrevIdx = maybe prevIdx id $ Map.lookup peer peerPrevIxes
        entries <- logEntries <$> get
        let peerPrevTerm = maybe prevTerm Lib.logTerm $ Map.lookup peerPrevIdx entries
        -- Send everything _after_ their previous index
        let toSend = snd $ Map.split peerPrevIdx entries
        sendPeerRequest peer $ Lib.AppendEntries $ Lib.AppendEntriesReq thisTerm myId peerPrevTerm peerPrevIdx toSend
        let peerIdx' = maybe peerPrevIdx fst $ Map.lookupMax toSend
        return $ Map.insert peer peerIdx' lastSentTo


sendPeerRequest :: Lib.PeerName -> Lib.PeerRequest -> ProtoStateMachine ()
sendPeerRequest peer req = tell [PeerRequest peer req $ processPeerResponseMessage peer]
