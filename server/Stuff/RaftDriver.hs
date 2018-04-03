{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.RaftDriver
( runModel
, nextIdSTM
)
where


import qualified Stuff.Proto as Proto

import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Monad.Logger as Logger
import qualified Data.Functor.Identity as Identity
import qualified Control.Concurrent.STM as STM
import           Control.Concurrent.STM (STM)
import qualified Data.Map as Map
import qualified Data.Set as Set
import Control.Applicative ((<|>))
import qualified System.IO.Unsafe
import Control.Monad
import qualified System.Random as Random
import Data.Ratio ((%))
import Control.Monad.Trans.Class (lift)

import Stuff.Types
import Stuff.RaftModel

type STMPendingRespMap resp = STM.TVar (Map.Map (IdFor resp) (STM.TMVar resp))
type LoggedSTM a = Logger.WriterLoggingT STM.STM a

nextIds :: STM.TVar Int
nextIds = System.IO.Unsafe.unsafePerformIO $ STM.newTVarIO 0
{-# NOINLINE nextIds #-}

nextIdSTM :: STM.STM Int
nextIdSTM = do
    n <- STM.readTVar nextIds
    STM.writeTVar nextIds (succ n)
    return n

runModel :: Proto.PeerName
            -> RequestsQ Proto.ClientRequest Proto.ClientResponse
            -> RequestsQ Proto.PeerRequest Proto.PeerResponse
            -> STM.TQueue (ProtoStateMachine ())
            -> RequestsInQ () Tick
            -> STMReqChanMap Proto.PeerName Proto.PeerRequest Proto.PeerResponse (ProtoStateMachine ())
            -> IO ()
runModel myName modelQ peerReqInQ peerRespInQ ticks peerOuts = do
    stateRef <- STM.atomically $ STM.newTVar $ mkRaftState
    -- incoming requests _from_ clients
    pendingClientResponses <- STM.atomically $ STM.newTVar $ Map.empty
    -- requests _from_ peers that are due a response
    -- map of ids of requests from peer to their pending responses.
    pendingPeerResponses <- STM.atomically $ STM.newTVar $ Map.empty
    -- Responses that we are awaiting _from_ peers.

    logVar <- STM.atomically $ STM.newTVar []

    elTimeout <- (% 1000) <$> Random.randomRIO (2500, 3500) :: IO Time
    let aeTimeout = 1 :: Time
    putStrLn $ show ("Election timeout is", elTimeout, "append entries", aeTimeout)

    let protocolEnv = mkProtocolEnv myName <$> (Set.fromList . Map.keys <$> STM.readTVar peerOuts) <*> pure elTimeout <*> pure aeTimeout
    let processClientMessage = runLoggerSTM logVar $ processReqRespMessageSTM stateRef protocolEnv pendingClientResponses modelQ processClientReqRespMessage
    let processPeerRequest = runLoggerSTM logVar $ processReqRespMessageSTM stateRef protocolEnv pendingPeerResponses peerReqInQ processPeerRequestMessage
    let processTickMessage = runLoggerSTM logVar $ processMessageSTM stateRef protocolEnv ticks processTick
    let processPeerResponse = runLoggerSTM logVar $ processRespMessageSTM stateRef protocolEnv peerRespInQ


    forever $ do
        when False $ do
            env <- STM.atomically protocolEnv
            putStrLn $ "Env: " ++ show env
        (_st', outputs, logs) <- STM.atomically $ do
            outputs <- processClientMessage <|> processPeerRequest <|> processTickMessage <|> processPeerResponse
            sendMessages pendingClientResponses peerOuts pendingPeerResponses outputs
            st' <- STM.readTVar stateRef
            logs <- STM.swapTVar logVar []
            return (st', outputs, logs)
        Logger.runStderrLoggingT $ forM_ logs $ \(loc, src, lvl, s) -> Logger.monadLoggerLog loc src lvl s
        when False $ putStrLn $ "sent: " ++ show outputs

type LogLine = (Logger.Loc,Logger.LogSource,Logger.LogLevel,Logger.LogStr)

runLoggerSTM :: (STM.TVar [LogLine]) -> LoggedSTM a -> STM.STM a
runLoggerSTM out action = do
  (a, logs) <- Logger.runWriterLoggingT action
  STM.modifyTVar' out (++ logs)
  return a

processMessageSTM :: STM.TVar RaftState
                  -> STM.STM ProtocolEnv
                  -> RequestsInQ xid req
                  -> (xid -> req -> ProtoStateMachine ())
                  -> Logger.WriterLoggingT STM.STM [ProcessorMessage]
processMessageSTM stateRef envSTM reqQ process = do
    (sender, m) <- lift $ STM.readTQueue reqQ
    case m of
        Just msg -> do
            snd <$> (processActions envSTM stateRef $ process sender msg)
        Nothing -> return []

processRespMessageSTM :: STM.TVar RaftState
                               -> STM.STM ProtocolEnv
                               -> STM.TQueue (ProtoStateMachine ())
                               -> LoggedSTM [ProcessorMessage]
processRespMessageSTM stateRef envSTM reqQ = do
    action <- lift $ STM.readTQueue reqQ
    snd <$> processActions envSTM stateRef action

processReqRespMessageSTM :: STM.TVar RaftState
                  -> STM.STM ProtocolEnv
                  -> STMPendingRespMap resp
                  -> RequestsQ req resp
                  -> (req -> IdFor resp -> ProtoStateMachine ())
                  -> LoggedSTM [ProcessorMessage]
processReqRespMessageSTM stateRef envSTM pendingResponses reqQ process = do
  (msg, pendingResponse) <- lift $ STM.readTQueue reqQ
  reqId <- lift $ IdFor <$> nextIdSTM
  lift $ STM.modifyTVar pendingResponses $ Map.insert reqId pendingResponse
  ((), toSend) <- processActions envSTM stateRef $ process msg reqId
  return toSend

sendMessages :: STMPendingRespMap Proto.ClientResponse
             -> STMReqChanMap Proto.PeerName Proto.PeerRequest Proto.PeerResponse (ProtoStateMachine ())
             -> STMPendingRespMap Proto.PeerResponse
             -> [ProcessorMessage]
             -> STM.STM ()
sendMessages pendingClientResponses peerRequests pendingPeerResponses toSend = do
            forM_ toSend $ \msg' -> do
                case msg' of
                    Reply respId reply -> sendPendingReply pendingClientResponses respId reply
                    PeerReply respId reply -> sendPendingReply pendingPeerResponses respId reply
                    PeerRequest peerName req k -> sendRequest peerRequests peerName req k
    where
        sendPendingReply mapping respId reply = do
          mvarp <- Map.lookup respId <$> STM.readTVar mapping
          STM.modifyTVar mapping $ Map.delete respId
          case mvarp of
              Just mvar -> STM.putTMVar mvar reply
              Nothing -> error $ "No mvar for response id " ++ show respId ++ " : " ++ show reply

        sendRequest mapping xid msg k = do
            queuep <- Map.lookup xid <$> STM.readTVar mapping
            case queuep of
                Just q -> do
                  STM.writeTQueue q $ (msg, k)
                Nothing -> error "what?"

processActions :: STM ProtocolEnv -> STM.TVar RaftState -> ProtoStateMachine a -> LoggedSTM (a, [ProcessorMessage])
processActions envSTM stateRef actions = do
  env <- lift $ envSTM
  s <- lift $ STM.readTVar stateRef
  let ((a, s', toSend), logs) = Identity.runIdentity $ Logger.runWriterLoggingT $ RWS.runRWST (runProto actions) env s
  forM_ logs $ \(loc, src, lvl, logmsg) -> Logger.monadLoggerLog loc src lvl logmsg
  lift $ STM.writeTVar stateRef s'
  return (a, toSend)
