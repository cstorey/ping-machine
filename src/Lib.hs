{-# LANGUAGE DeriveGeneric #-}

module Lib
    (
      ClientRequest(..)
    , ClientResponse(..)
    ) where

import GHC.Generics
import qualified Data.Binary as Binary

data ClientRequest =
    Bing
  | Ping
  deriving (Show, Generic)

data ClientResponse =
    Bong Int
  deriving (Show, Generic)

instance Binary.Binary ClientRequest
instance Binary.Binary ClientResponse