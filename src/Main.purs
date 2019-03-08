module Main where

import Prelude

import Data.Maybe (Maybe(..))
import Effect (Effect)
import InitialAccess (spec) as InitialAccess
import Ipfs.Api.Client (Client(..)) as Ipfs.Api.Client
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
  Spec.run' (Spec.defaultConfig { timeout = Just 5000 }) [ consoleReporter ] $
    InitialAccess.spec ipfs
