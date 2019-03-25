module Test.Main where

import Prelude
import Effect (Effect)
import Data.Maybe (Maybe(..))
import Effect.Console (log)
import Ipfs.Api.Client (Client(..)) as Ipfs.Api.Client
import StressTests (createLinearDAG, createBinaryTrees)
import Test.Spec.Reporter (consoleReporter)
import Test.Spec.Runner (run', defaultConfig) as Spec

main :: Effect Unit
main = do
  let
    ipfs :: Ipfs.Api.Client.Client
    ipfs =
      Ipfs.Api.Client.Client
        { baseUrl: "https://ipfs-x2.sylo.io"
        , modifyRequest: Nothing
        }
  {--Spec.run' (Spec.defaultConfig { timeout = Just 10000 }) [ consoleReporter ] $ do--}
    {--initialAccess ipfs--}
    {--stressPubsub ipfs--}
  Spec.run' (Spec.defaultConfig { timeout = Just 300000 }) [ consoleReporter ] $ do
    {--createLinearDAG ipfs--}
    createBinaryTrees ipfs
