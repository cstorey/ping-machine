{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Stuff.LinearizabilityCheckSpec (spec) where

import           Test.Hspec

import Data.Map (Map)
import qualified Data.Map as Map
import Control.Applicative ((<|>), empty)
-- import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (throwE, catchE, Except, runExcept)
import Data.Foldable (asum)
import Data.Semigroup ((<>), Semigroup)
import Debug.Trace as Trace
import qualified Data.Text.Lazy as Text
import qualified Data.Text.Format as Text
import qualified Data.Text.Format.Params as Text
import qualified Data.Sequence as Seq
import Data.Sequence (Seq, (|>), ViewL(..))

newtype Process = Process(Int)
    deriving (Show, Eq, Ord)

data HistoryElement req resp =
    Call req
  | Ret resp
    deriving (Show, Eq, Ord)

data Linearization req resp = Op Process req resp
    deriving (Show, Eq, Ord)

data RegisterReq a =
    RWrite a
  | RRead
    deriving (Show, Eq, Ord)

data RegisterRet a =
    ROk
  | RVal a
    deriving (Show, Eq, Ord)


data FifoReq a =
    FEnqueue a
  | FDequeue
    deriving (Show, Eq, Ord)
data FifoRet a =
    FOk
  | FVal a
  | FEmpty
    deriving (Show, Eq, Ord)

type ModelFun s req res = (s -> req -> (s, res))

data NotLinearizableReason req res =
    ModelMismatch req res res
  | NonMatchInObservable (HistoryElement req res)
  | ExpectingOp
  deriving (Show, Eq, Ord)

data NotLinearizable req res = NotLinearizable {
  reason :: NotLinearizableReason req res
, linPrefix :: [Linearization req res]
}
  deriving (Show, Eq, Ord)

-- We want to find the "best" error here, so this is kinda heuristic?
instance (Show a, Show b) => Semigroup (NotLinearizable a b) where
  x <> y = case ((length $ linPrefix x) `compare` (length $ linPrefix y), reason x, reason y) of
    (GT, _, _) -> x
    (LT, _, _) -> y
    (EQ, ModelMismatch _ _ _, _) -> x
    (EQ, _, ModelMismatch _ _ _) -> y
    (EQ, NonMatchInObservable _, _) -> x
    (EQ, _, NonMatchInObservable _) -> y
    (EQ, _, _) -> y

instance (Show a, Show b) => Monoid (NotLinearizable a b) where
  mempty = NotLinearizable ExpectingOp []
  mappend = (<>)

a, b, c :: Process
a = Process 0
b = Process 1
c = Process 2


h1History :: [(Process, HistoryElement (FifoReq String) (FifoRet String))]
h1History =
  [ (a, Call $ FEnqueue "x")
  , (b, Call $ FEnqueue "y")
  , (b, Ret $ FOk)
  , (a, Ret $ FOk)
  , (b, Call $ FDequeue)
  , (b, Ret $ FVal "x")
  , (a, Call $ FDequeue)
  , (a, Ret $ FVal "y")
  ]

h1Linearisation :: [Linearization (FifoReq String) (FifoRet String)]
h1Linearisation =
  [ Op a (FEnqueue "x") FOk
  , Op b (FEnqueue "y") FOk
  , Op b FDequeue (FVal "x")
  , Op a FDequeue (FVal "y")
  ]


-- Invalid; as enqueue of "y" happens strictly after "x"
h2History :: [(Process, HistoryElement (FifoReq String) (FifoRet String))]
h2History =
  [ (a, Call $ FEnqueue "x")
  , (a, Ret $ FOk)
  , (b, Call $ FEnqueue "y")
  , (a, Call $ FDequeue)
  , (b, Ret $ FOk)
  , (a, Ret $ FVal "y")
  ]

-- Acceptable
h3History :: [(Process, HistoryElement (FifoReq String) (FifoRet String))]
h3History =
  [ (a, Call $ FEnqueue "x")
  , (b, Call $ FDequeue)
  , (a, Ret $ FOk)
  , (b, Ret $ FVal "x")
  ]

h3Linearisation :: [Linearization (FifoReq String) (FifoRet String)]
h3Linearisation =
  [ Op a (FEnqueue "x") FOk
  , Op b FDequeue (FVal "x")
  ]

-- Invalid; "y" shuold not be dequeued twice.
h4History :: [(Process, HistoryElement (FifoReq String) (FifoRet String))]
h4History =
  [ (a, Call $ FEnqueue "x")
  , (b, Call $ FEnqueue "y")
  , (a, Ret $ FOk)
  , (b, Ret $ FOk)

  , (b, Call $ FDequeue)
  , (c, Call $ FDequeue)
  , (b, Ret $ FVal "y")
  , (c, Ret $ FVal "y")
  ]

-- Acceptable
h5History :: [(Process, HistoryElement (RegisterReq Int) (RegisterRet Int))]
h5History =
  [ (a, Call (RWrite 0))
  , (a, Ret ROk)
  , (b, Call (RWrite 1))
  , (a, Call RRead)
  , (a, Ret (RVal 1))
  , (c, Call (RWrite 0))
  , (c, Ret ROk)
  , (b, Ret ROk)
  , (b, Call RRead)
  , (b, Ret (RVal 0))
  ]

h5Linearisation :: [Linearization (RegisterReq Int) (RegisterRet Int)]
h5Linearisation =
  [  Op a (RWrite 0) ROk
  ,  Op b (RWrite 1) ROk
  ,  Op a RRead (RVal 1)
  ,  Op c (RWrite 0) ROk
  ,  Op b RRead (RVal 0)
  ]

-- Invalid; `c`'s `(Write 0) -> Ok` happens strictly before `b`'s `Read -> Val 1`
h6History :: [(Process, HistoryElement (RegisterReq Int) (RegisterRet Int))]
h6History =
  [ (a, Call (RWrite 0))
  , (a, Ret ROk)
  , (b, Call (RWrite 1))
  , (a, Call RRead)
  , (a, Ret (RVal 1))
  , (c, Call (RWrite 0))
  , (c, Ret ROk)
  , (b, Ret ROk)
  , (b, Call RRead)
  , (b, Ret (RVal 1))
  ]
checkHistory :: forall req res s. (Eq req, Eq res, Show req, Show res, Show s)
             => ModelFun s req res
             -> s
             -> [(Process, HistoryElement req res)]
             -> Either (NotLinearizable req res) [Linearization req res]
checkHistory model initialState h = runExcept $ go Map.empty Map.empty initialState 0 h
  where
    go :: Map Process req
       -> Map Process (req, res)
       -> s
       -> Int
       -> [(Process, HistoryElement req res)]
       -> Except (NotLinearizable req res) [Linearization req res]
    go calls rets s depth history = doneRule <|> observationRule <|> linRule
      where
      _spaces = take (depth * 2) $ cycle " "
      doneRule = do
        -- Trace.traceM (Text.unpack $ Text.format "done? calls:{}; rets: {}; pending:{}" (Text.Shown $ Map.size calls, Text.Shown $ Map.size rets, Text.Shown $ Map.map length histories))
        if history == [] && Map.null calls && Map.null rets
        then return []
        else empty

        -- From Testing from "Testing for Linearizability", Gavin Lowe
        -- rule `call`
      observationRule = do
        -- _trace "{}buf: calls:{}; rets: {}" (_spaces, _s calls, _s rets)
        case history of
          (p, op) : future -> do
            -- _trace "{}observation History: this:{}; future:{}" (_spaces, _s (p, op), _s future)
            let call = Map.lookup p calls
            let ret = Map.lookup p rets
            case (op, call, ret) of
              (Call req, Nothing, Nothing) -> do
                -- _trace "{}call:{}: req:{}" (_spaces, _s p, _s req)
                rest <- go (Map.insert p req calls) rets s (succ depth) future
                -- _trace "{}buf: calls:{}; rets: {}" (_spaces, _s calls, _s rets)
                -- _trace "{}call:{}: req:{}; <- {}" (_spaces, _s p, _s req, _s rest)
                return rest
              (Ret res, Nothing, Just (_call, expected)) | (res == expected) -> do
                -- _trace "{}Ret:{}; {} -> {}" (_spaces, _s p, _s _call, _s res)
                rest <- go calls (Map.delete p rets) s (succ depth) future
                -- Something something check for wall-clock time.
                -- _trace "{}buf: calls:{}; rets: {}" (_spaces, _s calls, _s rets)
                -- _trace "{}Ret:{}; {} - {}; <- {}..." (_spaces, _s p, _s _call, _s res, _s rest)
                return $ rest
              (Ret res, Nothing, Just (_call, expected)) -> do
                throwE $ NotLinearizable (ModelMismatch _call expected res) []
              _ -> do
                -- _trace "{}???: {}" (_spaces, _s _other)
                throwE $ NotLinearizable (NonMatchInObservable op) []
          [] -> do
                -- _trace "{}noFuture: {}" (_spaces, "" :: String)
                throwE $ NotLinearizable (ExpectingOp) []

      linRule = do
        -- _trace "{}linRule{}" (_spaces, ("" :: String))
        -- _trace "{}buf: calls:{}; rets: {}" (_spaces, _s calls, _s rets)
        rs <- asum $ flip map (Map.toList calls) $ \(p, req) -> do
          --  (startTime, req) <- maybe empty pure $ Map.lookup p calls
          let (s', ret) = model s req
          -- _trace "lin@{}: state: {}: req:{} -> expected:{}" (_s p, _s s, _s req, _s ret)
          let lin = Op p req ret
          rest <- go (Map.delete p calls) (Map.insert p (req, ret) rets) s' (succ depth) history
            `catchE` prefixErrorWith lin

          -- _trace "buf: calls:{}; rets: {}" (_s calls, _s rets)
          -- _trace "lin:{}: req:{} -> expected:{}; <- {} : {}" (_s p, _s req, _s ret, _s lin, _s rest)
          return $ lin : rest

        -- _trace "{}linCandidates: {}" (_spaces, _s rs)
        return rs
      prefixErrorWith :: Linearization req res -> NotLinearizable req res -> Except (NotLinearizable req res) a
      prefixErrorWith lin err = throwE $ err { linPrefix = lin : linPrefix err }

_trace :: (Text.Params ps0, Applicative f) => Text.Format -> ps0 -> f ()
_trace fmt ps = Trace.traceM (Text.unpack $ f fmt ps)
  where
    f = Text.format

_s :: a -> Text.Shown a
_s = Text.Shown

spec :: Spec
spec = do
  describe "Examples from Herlihy and Wing" $ do
    it "Linearizes h1" $ do
      checkHistory fifo newFifo h1History `shouldBe` Right h1Linearisation
    it "Finds h2 Invalid" $ do
      let result = checkHistory fifo newFifo h2History
      result `shouldSatisfy` isFailure
      -- These bits might be terribly fragile.
      err <- either return (fail . show) $ result

      linPrefix err `shouldStartWith` [Op (Process 0) (FEnqueue "x") FOk,Op (Process 0) FDequeue (FVal "x"),Op (Process 1) (FEnqueue "y") FOk]
      reason err `shouldBe` ModelMismatch FDequeue (FVal "x") (FVal "y")

    it "Linearizes h3" $ do
      checkHistory fifo newFifo h3History `shouldBe` Right h3Linearisation
    it "Finds h4 Invalid" $ do
      let result = checkHistory fifo newFifo h4History
      result `shouldSatisfy` isFailure
      -- These bits might be terribly fragile.
      err <- either return (fail . show) $ result
      linPrefix err `shouldBe` [
        Op (Process 0) (FEnqueue "x") FOk,Op (Process 1) (FEnqueue "y") FOk,
        Op (Process 1) FDequeue (FVal "x"),Op (Process 2) FDequeue (FVal "y")]
      reason err `shouldBe` ModelMismatch FDequeue (FVal "x") (FVal "y")

    it "Linearizes h5" $ do
      checkHistory register newRegister h5History `shouldBe` Right h5Linearisation
    it "Finds h6 Invalid" $ do
      let result = checkHistory register newRegister h6History
      result `shouldSatisfy` isFailure
      -- These bits might be terribly fragile.
      err <- either return (fail . show) $ result
      linPrefix err `shouldBe` [
          Op (Process 0) (RWrite 0) ROk,Op (Process 1) (RWrite 1) ROk,
          Op (Process 0) RRead (RVal 1), Op (Process 2) (RWrite 0) ROk,
          Op (Process 1) RRead (RVal 0)]
      reason err `shouldBe` ModelMismatch RRead (RVal 0) (RVal 1)

  where
    newRegister :: Int
    newRegister = (-1)
    register :: ModelFun a (RegisterReq a) (RegisterRet a)
    register state RRead = (state, RVal state)
    register _ (RWrite x) = (x, ROk)

    newFifo = Seq.empty
    fifo :: ModelFun (Seq a) (FifoReq a) (FifoRet a)
    fifo state (FEnqueue x) = (state |> x, FOk)
    fifo state FDequeue = case Seq.viewl state of
      val :< rest -> (rest, FVal val)
      EmptyL -> (state, FEmpty)

    isFailure = either (const True) (const False)
