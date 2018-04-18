{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Stuff.Models 
( ModelFun
, Model(..)
, RegisterReq(..)
, RegisterRet(..)
, FifoReq(..)
, FifoRet(..)
, BingBongReq(..)
, BingBongRet(..)
, bingBongModel
) where
import Data.Binary (Binary)
import GHC.Generics (Generic)

type ModelFun s req res = (s -> req -> (s, res))


data Model st req resp = Model {
  modelStep :: ModelFun st req resp
, modelInit :: st
}

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

data BingBongReq =
    Bing
  | Ping
  deriving (Eq, Ord, Show, Generic)
instance Binary BingBongReq
data BingBongRet =
    Bong (Maybe Int)
  deriving (Eq, Ord, Show, Generic)
instance Binary BingBongRet

bingBongModel :: Model Int BingBongReq BingBongRet 
bingBongModel = Model { 
  modelStep = \n _ -> (succ n, Bong (Just n))
, modelInit = 0
}
