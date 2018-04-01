import Stuff.ConsensusSpec (tests)
import           System.IO (BufferMode(..), hSetBuffering, stdout, stderr)
import           System.Exit (exitFailure)
import Control.Monad
import           Hedgehog

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  res <- checkParallel tests
  unless res $ exitFailure