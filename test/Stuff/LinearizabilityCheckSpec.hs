{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Stuff.LinearizabilityCheckSpec (spec) where

import           Test.Hspec

import Data.Map (Map)
import qualified Data.Map as Map
import Data.Foldable (foldl')
import Control.Applicative ((<|>), empty)
-- import Debug.Trace as Trace
-- import qualified Data.Text.Lazy as Text
-- import qualified Data.Text.Format as Text
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
    Write a
  | Read
    deriving (Show, Eq, Ord)

data RegisterRet a =
    Ok
  | Val a
    deriving (Show, Eq, Ord)

type ModelFun s req res = (s -> req -> (s, res))

a, b, c :: Process
a = Process 0
b = Process 1
c = Process 2

-- Acceptable
h5History :: [(Process, HistoryElement (RegisterReq Int) (RegisterRet Int))]
h5History =
  [ (a, Call (Write 0))
  , (a, Ret Ok)
  , (b, Call (Write 1))
  , (a, Call Read)
  , (a, Ret (Val 1))
  , (c, Call (Write 0))
  , (c, Ret Ok)
  , (b, Ret Ok)
  , (b, Call Read)
  , (b, Ret (Val 0))
  ]

h5Linearisation :: [Linearization (RegisterReq Int) (RegisterRet Int)]
h5Linearisation =
  [ Op a (Write 0) Ok
  , Op b (Write 1) Ok
  , Op a Read (Val 1)
  , Op c (Write 0) Ok
  , Op b Read (Val 0)
  ]

-- Not acceptable
h6History :: [(Process, HistoryElement (RegisterReq Int) (RegisterRet Int))]
h6History =
  [ (a, Call (Write 0))
  , (a, Ret Ok)
  , (b, Call (Write 1))
  , (a, Call Read)
  , (a, Ret (Val 1))
  , (c, Call (Write 0))
  , (c, Ret Ok)
  , (b, Ret Ok)
  , (b, Call Read)
  , (b, Ret (Val 1))
  ]
checkHistory :: forall req res s. (Eq req, Eq res, Show req, Show res, Show s)
             => ModelFun s req res
             -> s
             -> [(Process, HistoryElement req res)]
             -> Either () [Linearization req res]
checkHistory model initialState history = case go [] Map.empty Map.empty initialState byProcess of
    h : _ -> Right h
    _ -> Left ()
  where
    byProcess :: Map Process [HistoryElement req res]
    byProcess = foldl' (flip $ \(p, op) -> Map.insertWith (flip mappend) p [op]) Map.empty history

    go :: [Linearization req res]
       -> Map Process req
       -> Map Process res
       -> s
       -> Map Process [HistoryElement req res]
       -> [[Linearization req res]] -- Set of potential options
    go prev calls rets s histories = doneRule <|> callRule <|> linRule <|> retRule

      where
      candidates = foldMap (\(p, h) -> map (\op -> (p, op)) $ take 1 h) $ Map.toList histories
      doneRule =
        if candidates == [] && Map.null calls && Map.null rets
        then return prev
        else empty

        -- From Testing from "Testing for Linearizability", Gavin Lowe
        -- rule `call`
      callRule = do
        (p, op) <- candidates
        case op of
          Call req | Map.notMember p calls && Map.notMember p rets -> do
            -- Trace.trace (Text.unpack $ Text.format "call@{}: req:{}" (Text.Shown p, Text.Shown req)) $ return ()
            go prev (Map.insert p req calls) rets s $ Map.adjust (drop 1) p histories
          _ -> empty

      linRule = do
        -- Candidates means here that they have some history (in this case, we
        -- care about rets) outstanding.
        (p, _) <- candidates
        req <- maybe empty pure $ Map.lookup p calls
        let (s', ret) = model s req
        -- Trace.trace (Text.unpack $ Text.format "lin@{}: state: {}: req:{} -> expected:{}" (Text.Shown p, Text.Shown s, Text.Shown req, Text.Shown ret)) $ return ()
        go (Op p req ret : prev) (Map.delete p calls) (Map.insert p ret rets) s' histories

      retRule = do
        (p, op) <- candidates
        expected <- maybe empty pure $ Map.lookup p rets
        case op of
          Ret res | res == expected -> do
            -- Trace.trace (Text.unpack $ Text.format "ret@{}: res:{} == expected:{}" (Text.Shown p, Text.Shown res, Text.Shown expected)) $ return ()
            go prev calls (Map.delete p rets) s $ Map.adjust (drop 1) p histories
          _ -> empty

spec :: Spec
spec = do
  describe "Examples from Herlihy and Wing" $ do
    it "Linearizes h5" $ do
      pending
      (checkHistory register newRegister h5History :: Either () [Linearization (RegisterReq Int) (RegisterRet Int)]) `shouldBe` Right h5Linearisation
    it "Finds h6 Invalid" $ do
      pending
      checkHistory register newRegister h6History `shouldBe` Left ()

  where
    newRegister :: Int
    newRegister = (-1)
    register :: ModelFun a (RegisterReq a) (RegisterRet a)
    register state Read = (state, Val state)
    register _ (Write x) = (x, Ok)
