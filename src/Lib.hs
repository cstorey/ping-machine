{-# LANGUAGE DeriveGeneric #-}

module Lib
    (
      ClientRequest(..)
    , ClientResponse(..)
    , PeerRequest(..)
    , PeerResponse(..)
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

data PeerRequest =
    IHave Int
  deriving (Show, Generic)
instance Binary.Binary PeerRequest

data PeerResponse =
    ThatsNiceDear Int
  deriving (Show, Generic)
instance Binary.Binary PeerResponse