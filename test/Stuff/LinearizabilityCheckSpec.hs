{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Stuff.LinearizabilityCheckSpec (spec) where

import           Test.Hspec

import Data.Map (Map)
import qualified Data.Map as Map
import Data.Foldable (foldl')
import Control.Applicative ((<|>), empty)
import Debug.Trace as Trace
import qualified Data.Text.Lazy as Text
import qualified Data.Text.Format as Text
import qualified Data.Text.Format.Params as Text
-- import qualified Data.Foldable as Foldable

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

type ModelFun s req res = (s -> req -> (s, res))

a, b, c :: Process
a = Process 0
b = Process 1
c = Process 2

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

h5Linearisation :: [(Integer, Linearization (RegisterReq Int) (RegisterRet Int))]
h5Linearisation =
  [ (0, Op a (RWrite 0) ROk) -- 0-1
  , (2, Op b (RWrite 1) ROk) -- 2-7
  , (3, Op a RRead (RVal 1)) -- 3-4
  , (5, Op c (RWrite 0) ROk) -- 5-6
  , (8, Op b RRead (RVal 0)) -- 8-9
  ]

-- Not acceptable
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
             -> Either () [(Integer, Linearization req res)]
checkHistory model initialState history = case go Map.empty Map.empty initialState byProcess of
    h : _ -> Right h
    _ -> Left ()
  where
    byProcess :: Map Process [(Integer, HistoryElement req res)]
    byProcess = foldl' (flip $ \(t, p, op) -> Map.insertWith (flip mappend) p [(t, op)]) Map.empty $ zipWith flatten3 [0..] history

    flatten3 :: a -> (b,c) -> (a,b,c)
    flatten3 x (y,z) = (x,y,z)

    go :: Map Process (Integer, req)
       -> Map Process (Integer, req, res)
       -> s
       -> Map Process [(Integer, HistoryElement req res)]
       -> [[(Integer, Linearization req res)]] -- Set of potential options
    go calls rets s histories = doneRule <|> callRule <|> linRule <|> retRule

      where
      candidates = foldMap (\(p, h) -> map (\(t, op) -> (t, p, op)) $ take 1 h) $ Map.toList histories
      doneRule = do
        -- Trace.traceM (Text.unpack $ Text.format "done? calls:{}; rets: {}; pending:{}" (Text.Shown $ Map.size calls, Text.Shown $ Map.size rets, Text.Shown $ Map.map length histories))
        if candidates == [] && Map.null calls && Map.null rets
        then return []
        else empty

        -- From Testing from "Testing for Linearizability", Gavin Lowe
        -- rule `call`
      callRule = do
        (t, p, op) <- candidates
        case op of
          Call req | Map.notMember p calls && Map.notMember p rets -> do
            -- _trace "call:{}@{}: req:{}" (_s p, _s t, _s req)
            go (Map.insert p (t, req) calls) rets s $ Map.adjust (drop 1) p histories
          _ -> empty

      linRule = do
        -- Candidates means here that they have some history (in this case, we
        -- care about rets) outstanding.
        (_, p, _) <- candidates
        (startTime, req) <- maybe empty pure $ Map.lookup p calls
        let (s', ret) = model s req
        -- _trace "lin@{}: state: {}: req:{} -> expected:{}" (_s p, _s s, _s req, _s ret)
        rest <- go (Map.delete p calls) (Map.insert p (startTime, req, ret) rets) s' histories
        let lin = Op p req ret
        -- _trace "lin:{}: req:{} -> expected:{}; <- {} : {}" (_s p, _s req, _s ret, _s (startTime, lin), _s rest)
        return $ (startTime, lin) : rest

      retRule = do
        (_endTime, p, op) <- candidates
        (_startTime, _call, expected) <- maybe empty pure $ Map.lookup p rets
        case op of
          Ret res | res == expected -> do
            -- Trace.traceM (Text.unpack $ Text.format "ret@{}: res:{} == expected:{}" (Text.Shown p, Text.Shown res, Text.Shown expected))
            rest <- go calls (Map.delete p rets) s $ Map.adjust (drop 1) p histories
            -- Something something check for wall-clock time.
            -- _trace "Ret:{}; {}:{} - {}:{}; <- {}..." (_s p, _s _startTime, _s _call, _s _endTime, _s res, _s rest)
            return $ rest
          Ret _res -> do
            -- Trace.traceM (Text.unpack $ Text.format "ret@{}: res:{} != expected:{}" (Text.Shown p, Text.Shown _res, Text.Shown expected))
            empty
          _ -> empty

_trace :: (Text.Params ps0, Applicative f) => Text.Format -> ps0 -> f ()
_trace fmt ps = Trace.traceM (Text.unpack $ f fmt ps)
  where
    f = Text.format

_s :: a -> Text.Shown a
_s = Text.Shown

spec :: Spec
spec = do
  describe "Examples from Herlihy and Wing" $ do
    it "Linearizes h5" $ do
      checkHistory register newRegister h5History `shouldBe` Right h5Linearisation
    xit "Finds h6 Invalid" $ do
      {-
        If I say `shouldBe` Right h5Linearisation; we see:

        expected: Right [Op (Process 0) (RWrite 0) ROk,Op (Process 1) (RWrite 1) ROk,Op (Process 0) RRead (RVal 1),Op (Process 2) (RWrite 0) ROk,Op (Process 1) RRead (RVal 0)]
        but got:  Right [Op (Process 0) (RWrite 0) ROk,Op (Process 1) (RWrite 1) ROk,Op (Process 0) RRead (RVal 1),Op (Process 1) RRead (RVal 1),Op (Process 2) (RWrite 0) ROk]

        So the final RWrite from `c` and RRead from `b` end up getting re-ordered. So, clearly, we have a missing constraint on the concrete ordering.
      -}
      checkHistory register newRegister h6History `shouldBe` Left ()

  where
    newRegister :: Int
    newRegister = (-1)
    register :: ModelFun a (RegisterReq a) (RegisterRet a)
    register state RRead = (state, RVal state)
    register _ (RWrite x) = (x, ROk)
