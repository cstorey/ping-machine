{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Server where

import qualified Lib

import qualified Network.Socket            as S
import qualified System.IO.Streams         as Streams
import qualified System.IO.Streams.Binary  as BStreams
import qualified Data.Binary as Binary
import qualified System.Environment as Env
import qualified Control.Monad.Trans.RWS.Strict as RWS
import           Control.Monad.Writer.Class (MonadWriter(..))
import           Control.Monad.State.Class (MonadState(..), modify)
import           Control.Monad.Reader.Class (MonadReader(..), asks)
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.Async as Async
import qualified Data.Map as Map
import qualified Debug.Trace as Trace
import Data.List ((\\))
import Control.Applicative ((<|>))
import qualified Data.Time.Clock.POSIX as Clock
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import System.Random as Random

-- import qualified Data.ByteString as B

import qualified Control.Concurrent as C
import Control.Monad
import qualified Control.Exception as E

type ResponsesOutQ resp =  STM.TQueue (Maybe resp)
type RequestsInQ sender req =  STM.TQueue (sender, Maybe req)
type RequestsOutQ req =  STM.TQueue (Maybe req)


newtype ClientID = ClientID Int
    deriving (Show, Eq, Ord)

-- Identifier for an _incoming_ request
newtype PeerID = PeerID Int
    deriving (Show, Eq, Ord)

data ProcessorMessage = Reply ClientID Lib.ClientResponse
    | PeerRequest Lib.PeerName Lib.PeerRequest
    | PeerReply PeerID Lib.PeerResponse
    deriving (Show)

type STMRespChanMap xid resp = STM.TVar (Map.Map xid (ResponsesOutQ resp))

type Time = Double
data Tick = Tick Time

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
,   pendingClientRequests :: Map.Map Lib.LogIdx ClientID
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

oneSecondMicroSeconds :: Int
oneSecondMicroSeconds = 1000000

timeout :: Time
timeout = oneSecond * 3

now :: IO Time
now = fromRational . toRational <$> Clock.getPOSIXTime

main ::  IO ()
main = S.withSocketsDo $ do
    clientPort : peerPort : peerPorts <- Env.getArgs
    let myName = Lib.PeerName peerPort
    clientAddr <- resolve clientPort
    peerListenAddr <- resolve peerPort
    ids <- STM.atomically $ STM.newTVar 0
    clientReqQ <- STM.atomically STM.newTQueue:: IO (STM.TQueue (ClientID,Maybe Lib.ClientRequest))
    peerReqInQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (PeerID,Maybe Lib.PeerRequest))
    peerRespQ <- STM.atomically STM.newTQueue :: IO (STM.TQueue (Lib.PeerName,Maybe Lib.PeerResponse))
    ticks <- STM.atomically STM.newTQueue :: IO (STM.TQueue ((),Maybe Tick))
    clients <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map ClientID (ResponsesOutQ Lib.ClientResponse)))
    requestToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map Lib.PeerName (ResponsesOutQ Lib.PeerRequest)))
    responsesToPeers <- STM.atomically $ STM.newTVar $ Map.empty :: IO (STM.TVar (Map.Map PeerID (ResponsesOutQ Lib.PeerResponse)))
    -- We also need to start a peer manager. This will start a single process
    -- for each known peer, attempt to connect, then relay messages to/from
    -- peers.
    let race = Async.race_
    (runListener (ClientID <$> nextId ids) clientAddr clients clientReqQ)
        `race` (runListener (PeerID <$> nextId ids) peerListenAddr responsesToPeers peerReqInQ)
        `race` (runOutgoing (Lib.PeerName <$> peerPorts) requestToPeers peerRespQ)
        `race` (runTicker ticks)
        `race` (runModel myName clientReqQ peerReqInQ peerRespQ clients ticks requestToPeers responsesToPeers)

nextId :: STM.TVar Int -> IO Int
nextId ids = STM.atomically $ do
    n <- STM.readTVar ids
    STM.writeTVar ids (n+1)
    return n


-- Supervisor
runOutgoing :: [Lib.PeerName]
            -> STMRespChanMap Lib.PeerName Lib.PeerRequest
            -> RequestsInQ Lib.PeerName Lib.PeerResponse
            -> IO ()
runOutgoing seedPeers peers peerRespQ = do
    putStrLn $ "peers:" ++ show seedPeers
    processes <- STM.atomically $ STM.newTVar $ Map.empty

    void $ forever $ do
        runningProcesses <- Map.elems <$> STM.readTVarIO processes
        let toStart = (seedPeers \\ runningProcesses)
        putStrLn $ "To start: " ++ show toStart
        forM_ toStart $ \name -> do
            q <- STM.newTQueueIO
            putStrLn $ "Starting: " ++ show name
            a <- Async.async $ runPeer peerRespQ name q
            STM.atomically $ do
                STM.modifyTVar peers $ Map.insert name q
                STM.modifyTVar processes $ Map.insert a name

        do
            procNames <- Map.elems <$> STM.readTVarIO processes
            putStrLn $ "procs:" ++ show procNames

        (peerNamep, ret) <- STM.atomically $ do
            procs <- STM.readTVar processes
            (a, retOrError) <- Async.waitAnyCatchSTM $ Map.keys procs
            let namep = Map.lookup a procs
            STM.writeTVar processes $ Map.delete a procs
            return (namep, retOrError)

        putStrLn $ "runOutgoing failed process: " ++ show (peerNamep, ret)
        C.threadDelay $ oneSecondMicroSeconds
        putStrLn $ "runOutgoing : restarting"

connect :: S.AddrInfo -> IO S.Socket
connect addr = do
    sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    Trace.trace ("Connecting " ++ show sock ++ " to " ++ show addr) $ return ()
    (S.connect sock $ S.addrAddress addr) `E.onException` do
        Trace.trace ("Failed " ++ show sock ++ " to "  ++ show addr) $ return ()
        S.close sock
    Trace.trace ("Connected " ++ show sock ++ " to "  ++ show addr) $ return ()
    return sock

runPeer :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp) => RequestsInQ Lib.PeerName req -> Lib.PeerName -> ResponsesOutQ resp -> IO ()
runPeer fromPeerQ name toPeerQ = do
    Trace.trace ("lookup peer " ++ show name) $ return ()
    addrInfo <- resolve $ Lib.unPeerName name
    Trace.trace ("initiate open " ++ show name) $ return ()
    (is, os) <- streamsOf =<< connect addrInfo

    Trace.trace ("Talking to peer " ++ show name) $ return ()
    processClientRequests fromPeerQ name toPeerQ (is, os)
    Trace.trace ("Finished with peer " ++ show name) $ return ()

runListener :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp, Show xid, Ord xid)
            => IO xid
            -> S.AddrInfo
            -> STM.TVar (Map.Map xid (ResponsesOutQ resp))
            -> RequestsInQ xid req
            -> IO ()
runListener newId addr clients reqs =
    E.bracket (listenFor addr) S.close (runAcceptor newId $ handleConn clients reqs)

resolve :: S.ServiceName -> IO S.AddrInfo
resolve port = do
    let hints = S.defaultHints {
            S.addrFlags = [S.AI_PASSIVE]
        , S.addrSocketType = S.Stream
        }
    addr:_ <- S.getAddrInfo (Just hints) Nothing (Just port)
    return addr

listenFor :: S.AddrInfo -> IO S.Socket
listenFor addr = do
    sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    S.setSocketOption sock S.ReuseAddr 1
    S.bind sock (S.addrAddress addr)
    S.listen sock 10
    putStrLn . show =<< S.getSocketName sock
    return sock

streamsOf :: (Binary.Binary a, Binary.Binary b) => S.Socket -> IO (Streams.InputStream a, Streams.OutputStream b)
streamsOf client = do
    (is, os) <- (Streams.socketToStreams client)
    (,) <$> BStreams.decodeInputStream is <*> BStreams.encodeOutputStream os


runAcceptor :: (Binary.Binary req, Binary.Binary resp)
            => IO xid
            -> (xid -> (Streams.InputStream req, Streams.OutputStream resp) -> IO ())
            -> S.Socket
            -> IO ()
runAcceptor newId handler listener = go
    where
    go = void $ forever $ do
            (client, x) <- S.accept listener
            putStrLn $ show (client, x)
            n <- newId
            void $ C.forkIO $ do
                E.bracket (streamsOf client) (const $ S.close client) (handler n)

handleConn :: (Show req, Show resp, Show xid, Ord xid)
            => STM.TVar (Map.Map xid (ResponsesOutQ resp))
            -> RequestsInQ xid req
            -> xid
            -> (Streams.InputStream req, Streams.OutputStream resp)
            -> IO ()

handleConn clients modelQ clientId (is,os) = do
    q <- STM.atomically $ do
        q <- STM.newTQueue
        STM.modifyTVar clients $ Map.insert clientId q
        return q
    processClientRequests modelQ clientId q (is, os)

processClientRequests :: (Show xid, Show req, Show resp) =>
       RequestsInQ xid req
    -> xid
    -> STM.TQueue (Maybe resp)
    -> (Streams.InputStream req, Streams.OutputStream resp)
    -> IO ()

processClientRequests outQ clientId inQ (is, os) = do
    Async.concurrently_ (reader_ clientId inQ) (writer_ inQ)
    putStrLn $ "Done: " ++ show clientId
    where
    reader_ senderId writerQ = do
        it <- Streams.read is
        case it of
            Just msg -> do
                putStrLn $ "<- " ++ show clientId ++ ":" ++ show msg
                STM.atomically $ STM.writeTQueue outQ $ (senderId, Just (msg))
                reader_ senderId writerQ
            Nothing -> STM.atomically $ STM.writeTQueue writerQ Nothing

    writer_ sender =  do
        msg <- STM.atomically $ STM.readTQueue sender
        putStrLn $ "-> " ++ show clientId ++ ":" ++ show msg
        Streams.write msg os
        maybe (return ()) (const $ writer_ sender) msg


runTicker :: STM.TQueue ((), Maybe Tick) -> IO ()
runTicker ticks = void $ forever $ do
    t <- now
    STM.atomically $ STM.writeTQueue ticks ((), Just $ Tick t)
    putStrLn $ "Tick: " ++ show t
    sleepTime <- Random.getStdRandom $ Random.randomR (oneSec `div` 2, oneSec * 3 `div` 2 )
    C.threadDelay sleepTime
    where
    oneSec = 1000000

--- Model bits


newFollower :: RaftRole
newFollower = Follower $ FollowerState 0

runModel :: Lib.PeerName
            -> RequestsInQ ClientID Lib.ClientRequest
            -> RequestsInQ PeerID Lib.PeerRequest
            -> RequestsInQ Lib.PeerName Lib.PeerResponse
            -> STMRespChanMap ClientID Lib.ClientResponse
            -> RequestsInQ () Tick
            -> STMRespChanMap Lib.PeerName Lib.PeerRequest
            -> STMRespChanMap PeerID Lib.PeerResponse
            -> IO ()
runModel myName modelQ peerReqInQ peerRespInQ clients ticks peerOuts responsePeers = do
    stateRef <- STM.atomically $ STM.newTVar $ RaftState newFollower 0 Map.empty Nothing 0 Nothing

    let protocolEnv = ProtocolEnv myName <$> (Map.keys <$> STM.readTVar peerOuts)
    let processClientMessage = processMessageSTM stateRef protocolEnv modelQ processClientRequestMessage
    let processTickMessage = processMessageSTM stateRef protocolEnv ticks processTick
    let processPeerRequest = processMessageSTM stateRef protocolEnv peerReqInQ processPeerRequestMessage
    let processPeerResponse = processMessageSTM stateRef protocolEnv peerRespInQ processPeerResponseMessage


    forever $ do
        do
            env <- STM.atomically protocolEnv
            putStrLn $ "Env: " ++ show env
        (_st', outputs) <- STM.atomically $ do
            outputs <- processClientMessage <|> processPeerRequest <|> processTickMessage <|> processPeerResponse
            sendMessages clients peerOuts responsePeers outputs
            st' <- STM.readTVar stateRef
            return (st', outputs)
        -- putStrLn $ "state now: " ++ show st'
        putStrLn $ "sent: " ++ show outputs

processMessageSTM :: STM.TVar RaftState
                  -> STM.STM ProtocolEnv
                  -> RequestsInQ xid req
                  -> (xid -> req -> ProtoStateMachine ())
                  -> STM.STM [ProcessorMessage]
processMessageSTM stateRef envSTM reqQ process = do
    (sender, m) <- STM.readTQueue reqQ
    case m of
        Just msg -> do
            s <- STM.readTVar stateRef
            env <- envSTM
            let ((), s', toSend) = RWS.runRWS (runProto $ process sender msg) env s
            STM.writeTVar stateRef s'
            return toSend
        Nothing -> return []

sendMessages :: STMRespChanMap ClientID Lib.ClientResponse
             -> STMRespChanMap Lib.PeerName Lib.PeerRequest
             -> STMRespChanMap PeerID Lib.PeerResponse
             -> [ProcessorMessage]
             -> STM.STM ()
sendMessages clients peers responsePeers toSend = do
            forM_ toSend $ \msg' -> do
                case msg' of
                    Reply clientId reply -> sendTo clients clientId reply
                    PeerRequest peerName req -> sendTo peers peerName req
                    PeerReply peerId req -> sendTo responsePeers peerId req
    where
        sendTo mapping xid msg = do
            queuep <- Map.lookup xid <$> STM.readTVar mapping
            case queuep of
                Just q -> STM.writeTQueue q $ Just msg
                Nothing -> error "what?"


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

processClientRequestMessage :: ClientID -> Lib.ClientRequest -> ProtoStateMachine ()
processClientRequestMessage sender command = do
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
        tell [Reply sender $ Left $ Lib.NotLeader myLeader]

    recordPendingClientRequest idx leader = do
        return $ leader { pendingClientRequests = Map.insert idx sender $ pendingClientRequests leader}

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

processPeerRequestMessage :: PeerID -> Lib.PeerRequest -> ProtoStateMachine ()
processPeerRequestMessage sender (Lib.RequestVote candidateTerm candidateName candidateIdx) = do
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

processPeerRequestMessage sender
    _msg@(Lib.AppendEntries (Lib.AppendEntriesReq leaderTerm leaderName prevTerm prevIdx newEntries)) = do
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
        refuseAppendEntries thisTerm = tell [PeerReply sender $ Lib.AppendResult thisTerm False]
        ackAppendEntries :: Lib.Term -> ProtoStateMachine ()
        ackAppendEntries thisTerm = tell [PeerReply sender $ Lib.AppendResult thisTerm True]

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

resetElectionTimeout :: ProtoStateMachine ()
resetElectionTimeout = Trace.trace ("Reset election timeout?") $ return ()

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

processPeerResponseMessage sender _msg@(Lib.AppendResult _term granted) = do
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
                if granted
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
        let (canRespond, unCommitted) = Map.spanAntitone (>= idx) pending
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
        tell $ map (\p -> PeerRequest p req) peers
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
        let peerIdx = maybe prevIdx id $ Map.lookup peer peerPrevIxes
        entries <- logEntries <$> get
        -- Send everything _after_ their previous index
        let toSend = snd $ Map.split peerIdx entries
        let req = Lib.AppendEntries $ Lib.AppendEntriesReq thisTerm myId prevTerm prevIdx toSend
        tell $ [PeerRequest peer req]
        let peerIdx' = maybe peerIdx fst $ Map.lookupMax toSend
        return $ Map.insert peer peerIdx' lastSentTo