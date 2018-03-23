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

