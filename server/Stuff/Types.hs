module Stuff.Types
( ResponsesOutQ
, RequestsInQ
, RequestsOutQ
, RequestsQ
, ClientID(..)
, PeerID(..)
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
type STMReqChanMap xid req resp = STM.TVar (Map.Map xid (RequestsQ req resp))

type Time = Double
data Tick = Tick Time

