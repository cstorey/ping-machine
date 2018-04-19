{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.Proto
    ( ClientError(..)
    , ClientResponse
    , PeerRequest(..)
    , PeerResponse(..)
    , LogIdx(..)
    , LogEntry(..)
    , PeerName(..)
    , Term
    , AppendEntriesReq(..)
    , RequestVoteReq(..)
    , AppendEntriesResponse(..)
    ) where

import GHC.Generics
import qualified Data.Binary as Binary
import Data.Binary (Binary)
import qualified Data.Map as Map
import Data.Hashable (Hashable)

data ClientError =
    NotLeader (Maybe PeerName)
  deriving (Show, Generic)
instance Binary.Binary ClientError

type ClientResponse resp = Either ClientError resp

newtype Term = Term Int deriving (Show, Eq, Ord, Num, Generic, Enum)
instance Binary.Binary Term

-- Nothing signifies an empty log
-- Just x signifies the value at position x
newtype LogIdx = LogIdx { unLogIdx :: Maybe Int } deriving (Show, Eq, Ord, Generic)
instance Binary LogIdx

data LogEntry req = LogEntry {
  logTerm :: Term
, logValue :: req
} deriving (Show, Eq, Ord, Generic)

instance Binary a => Binary (LogEntry a)

-- Identifier for an outgoing request
newtype PeerName = PeerName { unPeerName :: String }
    deriving (Show, Eq, Ord, Generic)
instance Binary.Binary PeerName
instance Hashable PeerName

data AppendEntriesReq req = AppendEntriesReq {
  aeLeaderTerm :: Term
, aeLeaderName :: PeerName
, aePrevTerm :: Term
, aePrevIdx :: LogIdx
, aeNewEntries :: Map.Map LogIdx (LogEntry req)
} deriving (Show, Eq, Ord, Generic)
instance (Binary req) => Binary (AppendEntriesReq req)

data RequestVoteReq = RequestVoteReq {
  rvTerm :: Term
, rvName :: PeerName
, rvHead :: LogIdx
} deriving (Show, Generic)
instance Binary.Binary RequestVoteReq

data PeerRequest req =
    RequestVote RequestVoteReq
  | AppendEntries (AppendEntriesReq req)
  deriving (Show, Generic)
instance Binary req => Binary (PeerRequest req)

data AppendEntriesResponse = AppendEntriesResponse {
  aerTerm :: Term
, aerSucceeded :: Bool
, aerLogHead :: LogIdx
} deriving (Show, Generic)
instance Binary.Binary AppendEntriesResponse

data PeerResponse =
    VoteResult Term Bool
  | AppendResult AppendEntriesResponse
  deriving (Show, Generic)
instance Binary.Binary PeerResponse

data ReqResp a b = Request a
  | Resp b
  deriving (Show, Generic)
instance (Binary.Binary a, Binary.Binary b) => Binary.Binary (ReqResp a b)
