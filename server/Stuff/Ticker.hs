module Stuff.Ticker
( Ticker(..)
, withTicker
) where

import Control.Concurrent.STM (STM)
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.Async as Async
import qualified Data.Time.Clock.POSIX as Clock
import System.Random as Random
import qualified Control.Concurrent as C
import Control.Monad

import Stuff.Types

data Ticker = Ticker {
  waiter :: Async.Async ()
, tickerReceive :: STM ((), Maybe Tick)
}

now :: IO Time
now = toRational <$> Clock.getPOSIXTime

withTicker :: (Ticker  -> IO a) -> IO a
withTicker f = do
  ticks <- STM.atomically STM.newTQueue
  Async.withAsync (runTicker ticks) $ \ticker -> do
    Async.link ticker
    f $ Ticker ticker (receive ticks)

  where
  receive = STM.readTQueue 

runTicker :: STM.TQueue ((), Maybe Tick) -> IO ()
runTicker ticks = void $ forever $ do
    t <- now
    STM.atomically $ STM.writeTQueue ticks ((), Just $ Tick t)
    putStrLn $ "Tick: " ++ show t
    sleepTime <- Random.getStdRandom $ Random.randomR (oneSec `div` 2, oneSec * 3 `div` 2 )
    C.threadDelay sleepTime
    where
    oneSec = 1000000
