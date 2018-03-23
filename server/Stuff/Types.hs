module Stuff.Types
( ResponsesOutQ
, RequestsInQ
, RequestsOutQ
, ClientID(..)
, PeerID(..)
, STMRespChanMap
, Time
, Tick(..)
)
where

import qualified Control.Concurrent.STM as STM
import qualified Data.Map as Map

type ResponsesOutQ resp =  STM.TQueue (Maybe resp)
type RequestsInQ sender req =  STM.TQueue (sender, Maybe req)
type RequestsOutQ req =  STM.TQueue (Maybe req)

newtype ClientID = ClientID Int
    deriving (Show, Eq, Ord)

-- Identifier for an _incoming_ request
newtype PeerID = PeerID Int
    deriving (Show, Eq, Ord)

type STMRespChanMap xid resp = STM.TVar (Map.Map xid (ResponsesOutQ resp))

type Time = Double
data Tick = Tick Time

