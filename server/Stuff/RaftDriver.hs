{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.RaftDriver
( runModel
, nextIdSTM
, Listener(..)
, Outgoing(..)
)
where


import qualified Stuff.Proto as Proto
import qualified Stuff.Models as Models

import qualified Control.Monad.Trans.RWS.Strict as RWS
import qualified Control.Monad.Logger as Logger
import qualified Data.Functor.Identity as Identity
import qualified Control.Concurrent.STM as STM
import           Control.Concurrent.STM (STM)
import Data.Map (Map)
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
import Stuff.Ticker (Ticker)
import qualified Stuff.Ticker as Ticker

type STMPendingRespMap resp = STM.TVar (Map.Map (IdFor resp) (STM.TMVar resp))
type LoggedSTM a = Logger.WriterLoggingT STM.STM a

data Driver st req resp = Driver {
  driverState                  :: STM.TVar (RaftState req resp)
, driverProtocolEnv            :: STM (ProtocolEnv st req resp)
, driverLogs                   :: STM.TVar [LogLine]
, driverPendingClientResponses :: STMPendingRespMap (Proto.ClientResponse resp)
--, driverPeerOuts               :: STMReqChanMap Proto.PeerName (Proto.PeerRequest req) Proto.PeerResponse (ProtoStateMachine st req resp ())
, driverOutgoing               :: Outgoing req (ProtoStateMachine st req resp ())
, driverPendingPeerResponses   :: STMPendingRespMap Proto.PeerResponse
, driverClients                :: Listener req (Proto.ClientResponse resp)
, driverPeers                  :: Listener (Proto.PeerRequest req) Proto.PeerResponse
, driverTicker                 :: Ticker
}

data Listener req resp = Listener {
  listenerRequests :: RequestsQ req resp
}

data Outgoing req r = Outgoing {
  outgoingPeers :: STMReqChanMap Proto.PeerName (Proto.PeerRequest req) Proto.PeerResponse r
, outgoingResponses :: STM.TQueue r
}

nextIds :: STM.TVar Int
nextIds = System.IO.Unsafe.unsafePerformIO $ STM.newTVarIO 0
{-# NOINLINE nextIds #-}

nextIdSTM :: STM.STM Int
nextIdSTM = do
    n <- STM.readTVar nextIds
    STM.writeTVar nextIds (succ n)
    return n

runModel :: (Show req, Show resp)
            => Proto.PeerName
            -> Listener req (Proto.ClientResponse resp)
            -> Listener (Proto.PeerRequest req) Proto.PeerResponse
            -> Ticker
            -- -> STMReqChanMap Proto.PeerName (Proto.PeerRequest req) Proto.PeerResponse (ProtoStateMachine st req resp ())
            -> Outgoing req (ProtoStateMachine st req resp ())
            -> Models.ModelFun st req resp
            -> st
            -> IO ()
runModel myName clients peers ticker outgoing modelFn initState = do
    stateRef <- STM.newTVarIO $ mkRaftState :: IO (STM.TVar (RaftState req resp))
    -- incoming requests _from_ clients
    pendingClientResponses <- STM.newTVarIO $ Map.empty :: IO (STM.TVar (Map z x))
    -- requests _from_ peers that are due a response
    -- map of ids of requests from peer to their pending responses.
    pendingPeerResponses <- STM.newTVarIO $ Map.empty :: IO (STM.TVar (Map k a))
    -- Responses that we are awaiting _from_ peers.

    logVar <- STM.newTVarIO [] :: IO (STM.TVar [y])

    elTimeout <- (% 1000) <$> Random.randomRIO (2500, 3500) :: IO Time
    let aeTimeout = 1 :: Time
    putStrLn $ show ("Election timeout is", elTimeout, "append entries", aeTimeout)

    let protocolEnv = mkProtocolEnv myName <$> (Set.fromList . Map.keys <$> STM.readTVar (outgoingPeers outgoing)) <*> pure elTimeout <*> pure aeTimeout <*> pure initState <*> pure modelFn

    run $ Driver stateRef protocolEnv logVar pendingClientResponses outgoing pendingPeerResponses clients peers ticker



run :: (Show req, Show resp) => Driver st req resp -> IO ()
run self = forever $ do
        STM.atomically once
        Logger.runStderrLoggingT $ logFromVar (driverLogs self)
    where
    processClientMessage   = runLoggerSTM (driverLogs self) $ processReqRespMessageSTM self (driverPendingClientResponses self) (driverClients self) processClientReqRespMessage
    processPeerRequest     = runLoggerSTM (driverLogs self) $ processReqRespMessageSTM self (driverPendingPeerResponses self)   (driverPeers self) processPeerRequestMessage
    processTickMessage     = runLoggerSTM (driverLogs self) $ tickerSTM                self (driverTicker self) processTick
    processPeerResponse    = runLoggerSTM (driverLogs self) $ processRespMessageSTM    self

    once                   = do
                                outputs <- processClientMessage <|> processPeerRequest <|> processTickMessage <|> processPeerResponse
                                sendMessages (driverPendingClientResponses self) (outgoingPeers $ driverOutgoing self) (driverPendingPeerResponses self) outputs

    logFromVar logVar = do 
          logs <- lift $ STM.atomically $ STM.swapTVar logVar []
          forM_ logs $ \(loc, src, lvl, s) -> Logger.monadLoggerLog loc src lvl s

type LogLine = (Logger.Loc,Logger.LogSource,Logger.LogLevel,Logger.LogStr)

runLoggerSTM :: (STM.TVar [LogLine]) -> LoggedSTM a -> STM.STM a
runLoggerSTM out action = do
  (a, logs) <- Logger.runWriterLoggingT action
  STM.modifyTVar' out (++ logs)
  return a

tickerSTM :: Driver st req resp
                  -> Ticker
                  -> (() -> Tick -> ProtoStateMachine st req resp ())
                  -> Logger.WriterLoggingT STM.STM [ProcessorMessage st req resp]
tickerSTM self ticker process = do
    m <- lift $ Ticker.tickerReceive ticker
    snd <$> (processActions self $ process () m)

processRespMessageSTM :: Driver st req resp
                      -> LoggedSTM [ProcessorMessage st req resp]
processRespMessageSTM self = do
    action <- lift . STM.readTQueue . outgoingResponses . driverOutgoing $ self
    snd <$> processActions self action

processReqRespMessageSTM :: Driver st req resp
                  -> STMPendingRespMap outs
                  -> Listener ins outs
                  -> (ins -> IdFor outs -> ProtoStateMachine st req resp ())
                  -> LoggedSTM [ProcessorMessage st req resp]
processReqRespMessageSTM self pendingResponses listener process = do
  (msg, pendingResponse) <- lift $ STM.readTQueue $ listenerRequests listener
  reqId <- lift $ IdFor <$> nextIdSTM
  lift $ STM.modifyTVar pendingResponses $ Map.insert reqId pendingResponse
  ((), toSend) <- processActions self $ process msg reqId
  return toSend

sendMessages :: (Show resp)
             => STMPendingRespMap (Proto.ClientResponse resp)
             -> STMReqChanMap Proto.PeerName (Proto.PeerRequest req) Proto.PeerResponse (ProtoStateMachine st req resp ())
             -> STMPendingRespMap Proto.PeerResponse
             -> [ProcessorMessage st req resp]
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

processActions :: Driver st req resp -> ProtoStateMachine st req resp a -> LoggedSTM (a, [ProcessorMessage st req resp])
processActions self actions = do
  env <- lift $ envSTM
  s <- lift $ STM.readTVar stateRef
  let ((a, s', toSend), logs) = Identity.runIdentity $ Logger.runWriterLoggingT $ RWS.runRWST (runProto actions) env s
  forM_ logs $ \(loc, src, lvl, logmsg) -> Logger.monadLoggerLog loc src lvl logmsg
  lift $ STM.writeTVar stateRef s'
  return (a, toSend)
  where
  stateRef = (driverState self) 
  envSTM = (driverProtocolEnv self)


