{-# LANGUAGE DeriveGeneric #-}

module Lib
    (
      ClientRequest(..)
    , ClientResponse(..)
    , PeerRequest(..)
    , PeerResponse(..)
    , LogIdx
    , LogValue
    , LogEntry(..)
    , PeerName(..)
    , Term
    , AppendEntriesReq(..)
    ) where

import GHC.Generics
import qualified Data.Binary as Binary

data ClientRequest =
    Bing
  | Ping
  deriving (Show, Generic)
instance Binary.Binary ClientRequest

data ClientResponse =
    Bong Int
  deriving (Show, Generic)
instance Binary.Binary ClientResponse

type Term = Int
type LogIdx = Int
type LogValue = Int
data LogEntry = LogEntry {
  logTerm :: Term
, logValue :: LogValue
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
, aeNewEntries :: [LogEntry]
} deriving (Show, Eq, Ord, Generic)
instance Binary.Binary AppendEntriesReq

data PeerRequest =
    RequestVote Term PeerName LogIdx
  | AppendEntries AppendEntriesReq
  deriving (Show, Generic)
instance Binary.Binary PeerRequest

data PeerResponse =
    VoteResult Term Bool
  deriving (Show, Generic)
instance Binary.Binary PeerResponse

data ReqResp a b = Request a
  | Resp b
  deriving (Show, Generic)
instance (Binary.Binary a, Binary.Binary b) => Binary.Binary (ReqResp a b)