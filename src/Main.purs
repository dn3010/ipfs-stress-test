module Main where

import Prelude hiding (join)

import Agent (createArbitraryAgents, generateAgentMessages, subscribeAgentsToGroupTopics)
import Control.Monad.Fork.Class (fork)
import Control.Monad.MonadLogging (runStdoutLoggingT)
import Control.Monad.Reader.Trans (runReaderT)
import Data.Maybe (Maybe(..))
import Data.Traversable (traverse)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Effect.Ref (new)
import Effect.Ref (new) as Ref
import Ipfs.Api.Client (Client(..)) as Ipfs.Api.Client
import LogMetadata (createArbitraryLogs)
import Random.LCG (randomSeed)
import Test.QuickCheck.Gen (choose)

main :: Effect Unit
main = do
  let
    ipfs :: Ipfs.Api.Client.Client
    ipfs =
      Ipfs.Api.Client.Client
        { baseUrl: "https://ipfs-x2.sylo.io"
        , modifyRequest: Nothing
        }
    dagShape = {linear: 0.40, diamond: 0.35, fork: 0.25}

  refGenState <- do
    seed <- randomSeed
    let
      genState =
        {newSeed: seed, size: 100}
    new genState

  let
    numLogs =
      200
    initialLogDepth =
      6
    numAgents =
      100
    logsPerAgent =
      10
    delayGenerator =
      choose 2000.0 4000.0
  launchAff_ $
    runStdoutLoggingT (const identity) $ do
      runReaderT <@> {ipfs, refGenState, delayGenerator} $ do
        logMetadata <- createArbitraryLogs dagShape numLogs initialLogDepth
        agents      <- createArbitraryAgents logMetadata numAgents logsPerAgent

        refAgents <- liftEffect $ traverse Ref.new agents
        void $ fork $ subscribeAgentsToGroupTopics refAgents
        generateAgentMessages refAgents
