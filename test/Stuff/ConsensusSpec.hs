module Stuff.ConsensusSpec (spec) where

import Test.Hspec

import Stuff.RaftModel
import Stuff.Proto
-- import Data.Set (Set)
import qualified Data.Set as Set

spec :: Spec
spec = do
  describe "startup" $ do
    it "removes leading and trailing whitespace" $ do
      () `shouldBe` ()
    it "Starts in Follower mode" $ do
      let allPeers = Set.fromList $ fmap PeerName ["a", "b", "c"]
      let _self = (PeerName "a")
      let _env = (ProtocolEnv _self (Set.difference allPeers $ Set.singleton _self) :: ProtocolEnv)
      -- let _ = shouldBe id id :: IO ()
      
      pending
