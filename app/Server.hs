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
} deriving (Show)

data RaftState =
    Follower FollowerState
  | Candidate CandidateState
  | Leader LeaderState
    deriving (Show)

data ProtocolState = ProtocolState {
    currentRole :: RaftState
,   currentTerm :: Int
,   logEntries :: Map.Map Lib.LogIdx Lib.LogEntry
,   votedFor :: Maybe Lib.PeerName
,   prevTickTime :: Time
,   currentLeader :: Maybe Lib.PeerName
} deriving (Show)

newtype ProtoStateMachine a = ProtoStateMachine {
    runProto :: RWS.RWS ProtocolEnv [ProcessorMessage] ProtocolState a
} deriving (Monad, Applicative, Functor, MonadState ProtocolState, MonadWriter [ProcessorMessage], MonadReader ProtocolEnv)

oneSecond :: Time
oneSecond = 1

oneSecondNS :: Int
oneSecondNS = 1000000000

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
    (runListener (ClientID <$> nextId ids) clientAddr clients clientReqQ) `race`
        (runListener (PeerID <$> nextId ids) peerListenAddr responsesToPeers peerReqInQ) `race`
        (runOutgoing (Lib.PeerName <$> peerPorts) requestToPeers peerRespQ) `race`
        (runTicker ticks) `race`
        (runModel myName clientReqQ peerReqInQ peerRespQ clients ticks requestToPeers responsesToPeers)

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
        forM_ (seedPeers \\ runningProcesses) $ \name -> do
            q <- STM.newTQueueIO
            a <- Async.async $ runPeer peerRespQ name q
            STM.atomically $ do
                STM.modifyTVar peers $ Map.insert name q
                STM.modifyTVar processes $ Map.insert a name
            putStrLn $ "Starting: " ++ show name
        do
            procNames <- Map.elems <$> STM.readTVarIO processes
            putStrLn $ "procs:" ++ show procNames

        (peerNamep, ret) <- STM.atomically $ do
            procs <- STM.readTVar processes
            (a, retOrError) <- Async.waitAnyCatchSTM $ Map.keys procs
            let namep = Map.lookup a procs
            STM.writeTVar processes $ Map.delete a procs
            return (namep, retOrError)

        putStrLn $ "runOutgoing : " ++ show (peerNamep, ret)
        C.threadDelay $ oneSecondNS

connect :: S.AddrInfo -> IO S.Socket
connect addr = do
    sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    (S.connect sock $ S.addrAddress addr) `E.onException` S.close sock
    return sock

runPeer :: (Binary.Binary req, Show req, Binary.Binary resp, Show resp) => RequestsInQ Lib.PeerName req -> Lib.PeerName -> ResponsesOutQ resp -> IO ()
runPeer fromPeerQ name toPeerQ = do
    addrInfo <- resolve $ Lib.unPeerName name
    (is, os) <- streamsOf =<< connect addrInfo

    processClientRequests fromPeerQ name toPeerQ (is, os)

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


newFollower :: RaftState
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
    stateRef <- STM.atomically $ STM.newTVar $ ProtocolState newFollower 0 Map.empty Nothing 0 Nothing

    let protocolEnv = ProtocolEnv myName <$> (Map.keys <$> STM.readTVar peerOuts)
    let processClientMessage = processMessageSTM stateRef protocolEnv modelQ processClientRequestMessage
    let processTickMessage = processMessageSTM stateRef protocolEnv ticks processTick
    let processPeerRequest = processMessageSTM stateRef protocolEnv peerReqInQ processPeerRequestMessage
    let processPeerResponse = processMessageSTM stateRef protocolEnv peerRespInQ processPeerResponseMessage


    forever $ do
        do
            env <- STM.atomically protocolEnv
            putStrLn $ "Env: " ++ show env
        (st', outputs) <- STM.atomically $ do
            outputs <- processClientMessage <|> processPeerRequest <|> processTickMessage <|> processPeerResponse
            sendMessages clients peerOuts responsePeers outputs
            st' <- STM.readTVar stateRef
            return (st', outputs)
        putStrLn $ "state now: " ++ show st'
        putStrLn $ "sent: " ++ show outputs

processMessageSTM :: STM.TVar ProtocolState
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


appendToLog :: Lib.ClientRequest -> ProtoStateMachine ()
appendToLog command = do
    thisTerm <- currentTerm <$> get
    let entry = Lib.LogEntry thisTerm command
    (_, prevIdx) <- getPrevLogTermIdx
    modify $ \st -> st {
        logEntries = Map.insert (succ prevIdx) entry $ logEntries st
    }

processClientRequestMessage :: ClientID -> Lib.ClientRequest -> ProtoStateMachine ()
processClientRequestMessage sender command = do
    role <- currentRole <$> get
    case role of
        Leader _ -> do
            appendToLog command
        _ -> do
            refuseClientRequest

    where
    refuseClientRequest = do
        myLeader <- currentLeader <$> get
        Trace.trace "Not leader" $ return ()
        tell [Reply sender $ Left $ Lib.NotLeader myLeader]

laterTermObserved :: Lib.Term -> ProtoStateMachine ()
laterTermObserved laterTerm = do
    thisTerm <- currentTerm <$> get
    Trace.trace ("Later term observed: " ++ show laterTerm ++ " > " ++ show thisTerm) $ return ()
    prevTick <- prevTickTime <$> get
    modify $ \st -> st { currentTerm = laterTerm, votedFor = Nothing }
    Trace.trace "stepDownIfCandidateOrLeader " $ return ()
    modify $ \st -> st {
        currentRole = Follower $ FollowerState {
            lastLeaderHeartbeat = prevTick
        }
    }

getPrevLogTermIdx :: ProtoStateMachine (Lib.Term, Lib.LogIdx)
getPrevLogTermIdx = do
    myLog <- logEntries <$> get
    return $ maybe (0, 0) (\(idx, it) -> (idx, Lib.logTerm it)) $ Map.lookupMax myLog



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
            Trace.trace ("Granting vote") $ grantVote thisTerm
            -- Reply No?
        _ | Maybe.isNothing vote && candidateIdx >= logIdx -> do
            Trace.trace ("Granting vote") $ return ()
            grantVote thisTerm
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
            entries <- logEntries <$> get
            let prevItem = Map.lookup prevIdx entries
            if maybe False ((== prevTerm) . Lib.logTerm) prevItem
            then do
                Trace.trace ("Refusing as prev item was: " ++ show prevItem) $ refuseAppendEntries thisTerm
            else do
                prevTick <- prevTickTime <$> get
                let idx = succ prevIdx
                let (entries', _)  = Map.split idx entries
                let entries'' = Map.union entries' newEntries
                let follower' = follower {
                    lastLeaderHeartbeat = prevTick
                }
                modify $ \st -> st {
                    logEntries = entries'',
                    currentRole = Follower follower',
                    currentLeader = Just leaderName
                }
                ackAppendEntries thisTerm
                Trace.trace "Someething something apply committed entries" $ return ()

resetElectionTimeout :: ProtoStateMachine ()
resetElectionTimeout = Trace.trace ("Reset election timeout?") $ return ()

processPeerResponseMessage :: Lib.PeerName -> Lib.PeerResponse -> ProtoStateMachine ()
processPeerResponseMessage _sender _msg@(Lib.VoteResult _term granted) = do
    Trace.trace ("processPeerResponseMessage: " ++ show _msg) $ return ()
    role <- currentRole <$> get
    case role of
        Follower _ -> do
            Trace.trace ("vote recieved when follower? " ++ show _msg) $ return ()
        Candidate st -> do
            whenCandidate st
        Leader _st -> do
            Trace.trace ("vote recieved when leader? " ++ show _msg) $ return ()
    where
    whenCandidate candidate = do
        if granted
        then do
            let newCandidate = candidate {
                votesForMe = _sender : votesForMe candidate
            }

            let currentVotes = length $ votesForMe newCandidate

            neededVotes <- getMajority

            Trace.trace ("needed: " ++ show neededVotes ++ " have: " ++ show currentVotes) $ return ()

            let newRole = if currentVotes >= neededVotes
                then Leader $ LeaderState Map.empty Map.empty Nothing
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
            modify $ \st -> st { currentRole = Leader $ leader' }
        _st -> do
            Trace.trace ("append response recieved when non leader? " ++ show _msg) $ return ()
    where
    whenLeader leader = do
        if granted
        then do
            let lastSent = followerLastSent leader
            case Map.lookup sender lastSent of
                Just sentIdx -> do
                    let followerPrevIdx' = Map.insert sender sentIdx $ followerPrevIdx leader

                    majority <- getMajority

                    -- Find items acked by a ajority of servers.
                    -- So first derive the acked idxes in descending order
                    (_, myIdx ) <- getPrevLogTermIdx
                    let known = List.sortOn (0 -) $ myIdx : Map.elems followerPrevIdx'
                    let committedIdx = Maybe.listToMaybe $ List.drop (pred majority) known

                    let leader' = leader {
                        followerPrevIdx = followerPrevIdx'
                    ,   followerLastSent = Map.delete sender lastSent
                    ,   committed = committedIdx
                    }
                    Trace.trace ("commited index should be: " ++ show committedIdx) return ()
                    Trace.trace ("append response granted: " ++ show leader') return leader'
                _ -> Trace.trace ("Response to an unset message?") $ return leader
        else
            error $ "AppendResult failed! " ++ show leader



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
        if (t - lastLeaderHeartbeat follower) > timeout
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
        Trace.trace ("Leader Tick") $ return ()
        peers <- asks peerNames
        let lastSent = followerLastSent leader
        let peerPrevIxes = followerPrevIdx leader
        peerSentIxes <- foldM (updatePeer peerPrevIxes) lastSent peers

        modify $ \st -> st {
            currentRole = Leader $ leader { followerLastSent = peerSentIxes }
        }
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