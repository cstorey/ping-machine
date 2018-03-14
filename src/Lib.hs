{-# LANGUAGE DeriveGeneric #-}

module Lib
    (
      ClientRequest(..)
    , ClientResponse(..)
    , PeerMessage(..)
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

data PeerMessage =
    IHave Int
  | ThatsNiceDear Int
  deriving (Show, Generic)
instance Binary.Binary PeerMessage