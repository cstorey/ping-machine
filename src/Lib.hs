{-# LANGUAGE DeriveGeneric #-}

module Lib
    ( someFunc
    , Message(..)
    ) where

import GHC.Generics
import qualified Data.Binary as Binary

data Message =
    Bing
  | Bong
  deriving (Show, Generic)


instance Binary.Binary Message

someFunc :: IO ()
someFunc = putStrLn "someFunc"
