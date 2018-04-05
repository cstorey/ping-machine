{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stuff.Proto
    (
      ClientRequest(..)
    , ClientResult(..)
    , ClientError(..)
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
import qualified Data.Map as Map

data ClientRequest =
    Bing
  | Ping
  deriving (Eq, Ord, Show, Generic)
instance Binary.Binary ClientRequest

data ClientResult =
    Bong (Maybe Int)
  deriving (Eq, Ord, Show, Generic)
instance Binary.Binary ClientResult

data ClientError =
    NotLeader (Maybe PeerName)
  deriving (Show, Generic)
instance Binary.Binary ClientError

type ClientResponse = Either ClientError ClientResult

newtype Term = Term Int deriving (Show, Eq, Ord, Num, Generic, Enum)
instance Binary.Binary Term

-- Nothing signifies an empty log
-- Just x signifies the value at position x
newtype LogIdx = LogIdx { unLogIdx :: Maybe Int } deriving (Show, Eq, Ord, Generic)
instance Binary.Binary LogIdx

data LogEntry = LogEntry {
  logTerm :: Term
, logValue :: ClientRequest
} deriving (Show, Eq, Ord, Generic)

instance Binary.Binary LogEntry

-- Identifier for an outgoing request
newtype PeerName = PeerName { unPeerName :: String }
    deriving (Show, Eq, Ord, Generic)
instance Binary.Binary PeerName

data AppendEntriesReq = AppendEntriesReq {
  aeLeaderTerm :: Term
, aeLeaderName :: PeerName
, aePrevTerm :: Term
, aePrevIdx :: LogIdx
, aeNewEntries :: Map.Map LogIdx LogEntry
} deriving (Show, Eq, Ord, Generic)
instance Binary.Binary AppendEntriesReq

data RequestVoteReq = RequestVoteReq {
  rvTerm :: Term
, rvName :: PeerName
, rvHead :: LogIdx
} deriving (Show, Generic)
instance Binary.Binary RequestVoteReq

data PeerRequest =
    RequestVote RequestVoteReq
  | AppendEntries AppendEntriesReq
  deriving (Show, Generic)
instance Binary.Binary PeerRequest

data AppendEntriesResponse = AppendEntriesResponse {
  aerTerm :: Term
, aerSucceeded :: Bool
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
