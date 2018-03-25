{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.RaftDriver
( runModel
, nextIdSTM
)
where


import qualified Lib

import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Concurrent.STM as STM
import qualified Data.Map as Map
import Control.Applicative ((<|>))
import qualified System.IO.Unsafe
import Control.Monad

import Stuff.Types
import Stuff.RaftModel

type STMPendingRespMap resp = STM.TVar (Map.Map (IdFor resp) (STM.TMVar resp))

nextIds :: STM.TVar Int
nextIds = System.IO.Unsafe.unsafePerformIO $ STM.newTVarIO 0
{-# NOINLINE nextIds #-}

nextIdSTM :: STM.STM Int
nextIdSTM = do
    n <- STM.readTVar nextIds
    STM.writeTVar nextIds (succ n)
    return n

runModel :: Lib.PeerName
            -> RequestsQ Lib.ClientRequest Lib.ClientResponse
            -> RequestsQ Lib.PeerRequest Lib.PeerResponse
            -> STM.TQueue (ProtoStateMachine ())
            -> RequestsInQ () Tick
            -> STMReqChanMap Lib.PeerName Lib.PeerRequest Lib.PeerResponse (ProtoStateMachine ())
            -> IO ()
runModel myName modelQ peerReqInQ peerRespInQ ticks peerOuts = do
    stateRef <- STM.atomically $ STM.newTVar $ mkRaftState
    -- incoming requests _from_ clients
    pendingClientResponses <- STM.atomically $ STM.newTVar $ Map.empty
    -- requests _from_ peers that are due a response
    -- map of ids of requests from peer to their pending responses.
    pendingPeerResponses <- STM.atomically $ STM.newTVar $ Map.empty
    -- Responses that we are awaiting _from_ peers.

    let protocolEnv = ProtocolEnv myName <$> (Map.keys <$> STM.readTVar peerOuts)
    let processClientMessage = processReqRespMessageSTM stateRef protocolEnv pendingClientResponses modelQ processClientReqRespMessage
    let processPeerRequest = processReqRespMessageSTM stateRef protocolEnv pendingPeerResponses peerReqInQ processPeerRequestMessage
    let processTickMessage = processMessageSTM stateRef protocolEnv ticks processTick
    let processPeerResponse = processRespMessageSTM stateRef protocolEnv peerRespInQ


    forever $ do
        when False $ do
            env <- STM.atomically protocolEnv
            putStrLn $ "Env: " ++ show env
        (_st', outputs) <- STM.atomically $ do
            outputs <- processClientMessage <|> processPeerRequest <|> processTickMessage <|> processPeerResponse
            sendMessages pendingClientResponses peerOuts pendingPeerResponses outputs
            st' <- STM.readTVar stateRef
            return (st', outputs)
        when False $ putStrLn $ "state now: " ++ show _st'
        when False $ putStrLn $ "sent: " ++ show outputs

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

processRespMessageSTM :: STM.TVar RaftState
                               -> STM.STM ProtocolEnv
                               -> STM.TQueue (ProtoStateMachine ())
                               -> STM.STM [ProcessorMessage]
processRespMessageSTM stateRef envSTM reqQ = do
    action <- STM.readTQueue reqQ
    s <- STM.readTVar stateRef
    env <- envSTM
    let ((), s', toSend) = RWS.runRWS (runProto $ action) env s
    STM.writeTVar stateRef s'
    return toSend

processReqRespMessageSTM :: STM.TVar RaftState
                  -> STM.STM ProtocolEnv
                  -> STMPendingRespMap resp
                  -> RequestsQ req resp
                  -> (req -> IdFor resp -> ProtoStateMachine ())
                  -> STM.STM [ProcessorMessage]
processReqRespMessageSTM stateRef envSTM pendingResponses reqQ process = do
  (msg, pendingResponse) <- STM.readTQueue reqQ
  reqId <- IdFor <$> nextIdSTM
  STM.modifyTVar pendingResponses $ Map.insert reqId pendingResponse

  s <- STM.readTVar stateRef
  env <- envSTM
  let ((), s', toSend) = RWS.runRWS (runProto $ process msg reqId) env s
  STM.writeTVar stateRef s'
  return toSend

sendMessages :: STMPendingRespMap Lib.ClientResponse
             -> STMReqChanMap Lib.PeerName Lib.PeerRequest Lib.PeerResponse (ProtoStateMachine ())
             -> STMPendingRespMap Lib.PeerResponse
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


