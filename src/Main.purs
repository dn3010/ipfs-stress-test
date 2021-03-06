module Main where

import Prelude hiding (join)

import Agent (RefPubsubRecords, createArbitraryAgents, generateAgentMessages, log2AgentsLookupTable, subscribeAgentsToGroupTopics)
import Control.Monad.Fork.Class (fork)
import Control.Monad.MonadLogging (runStdoutLoggingT)
import Control.Monad.Reader.Trans (runReaderT)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (NonEmpty, (:|))
import Data.Traversable (traverse)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Effect.Ref (new)
import Effect.Ref (new) as Ref
import Ipfs.Api.Client (Client(..)) as Ipfs.Api.Client
import LogMetadata (createArbitraryLogs)
import Random.LCG (randomSeed)
import Test.QuickCheck.Gen (choose, elements)

main :: Effect Unit
main = do
  let
    ipfsDestinations :: NonEmpty Array Ipfs.Api.Client.Client
    ipfsDestinations =
      Ipfs.Api.Client.Client
        { baseUrl: "https://sylo.mysinglesource.io"
        , modifyRequest: Nothing
        }
        :|
        [ Ipfs.Api.Client.Client
            { baseUrl: "https://ipfs-cluster1.sylo.io"
            , modifyRequest: Nothing
            }
        , Ipfs.Api.Client.Client
            { baseUrl: "https://ipfs-cluster2.sylo.io"
            , modifyRequest: Nothing
            }
        ]
    ipfsDestChooser =
      elements ipfsDestinations
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
      2
    numAgents =
      100
    logsPerAgent =
      10
    delayChooser =
      choose 2000.0 3000.0
  launchAff_ $
    runStdoutLoggingT (const identity) $ do
      runReaderT <@> {refGenState, ipfsDestChooser, delayChooser} $ do
        logMetadata <- createArbitraryLogs dagShape numLogs initialLogDepth
        agents      <- createArbitraryAgents logMetadata numAgents logsPerAgent

        refAgents <- liftEffect $ traverse Ref.new agents
        refPubsub :: RefPubsubRecords <- liftEffect $ Ref.new []

        void $ fork $ subscribeAgentsToGroupTopics refAgents refPubsub
        generateAgentMessages
          refAgents
          refPubsub
          (log2AgentsLookupTable agents)
