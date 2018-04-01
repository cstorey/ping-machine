module Stuff.Types
( ResponsesOutQ
, RequestsInQ
, RequestsOutQ
, RequestsQ
, ClientID(..)
, PeerID(..)
, OutgoingReqQ
, STMReqChanMap
, Time
, Tick(..)
)
where

import qualified Control.Concurrent.STM as STM
import qualified Data.Map as Map

type ResponsesOutQ resp =  STM.TQueue (Maybe resp)
type RequestsInQ sender req =  STM.TQueue (sender, Maybe req)
type RequestsOutQ req =  STM.TQueue (Maybe req)

type RequestsQ req resp =  STM.TQueue (req, STM.TMVar resp)

newtype ClientID = ClientID Int
    deriving (Show, Eq, Ord)

-- Identifier for an _incoming_ request
newtype PeerID = PeerID Int
    deriving (Show, Eq, Ord)

--
type OutgoingReqQ req resp r = (STM.TQueue (req, resp -> r))
type STMReqChanMap xid req resp r = STM.TVar (Map.Map xid (OutgoingReqQ req resp r))

type Time = Rational
data Tick = Tick Time