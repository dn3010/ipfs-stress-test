module Agent
  ( Agent
  , createArbitraryAgents
  , subscribeAgentsToGroupTopics
  , generateAgentMessages
  ) where

import Common (IpfsGenContext, expectRight, shorten)
import Control.Monad.Except.Trans (runExceptT)
import Control.Monad.Gen (elements)
import Control.Monad.MonadLogging (d) as L
import Control.Monad.MonadLogging (runNoLoggingT)
import Control.Monad.Reader.Class (ask)
import Control.Monad.Rec.Class (forever)
import Control.Monad.Trans.Class (lift)
import Control.Parallel (parTraverse_)
import Data.Argonaut (decodeJson, jsonParser, stringify)
import Data.Argonaut.Encode.Class (encodeJson)
import Data.Array (filter, range)
import Data.Array (take, (:)) as A
import Data.Foldable (for_, notElem)
import Data.List.NonEmpty (fromList, singleton) as NEL
import Data.Map.Internal (Map, fromFoldable, keys, toUnfoldable)
import Data.Map.Internal (insert, lookup) as Map
import Data.Maybe (Maybe, isJust, maybe)
import Data.NonEmpty (singleton) as NE
import Data.String.Utils (fromCharArray)
import Data.Time.Duration (Milliseconds(..))
import Data.Traversable (for)
import Data.Tuple (Tuple)
import Data.Tuple.Nested ((/\))
import Data.UUID (UUID, emptyUUID, genUUID)
import Debug.Trace (traceM)
import Effect.Aff (apathize, delay, forkAff)
import Effect.Aff.Class (liftAff)
import Effect.Class (liftEffect)
import Effect.Ref (Ref, read, write)
import Effect.Ref (read, write) as Ref
import Entry (E, arbitraryEntry)
import IPFSLog.EntryStore.Class (createEntry, getEntry) as EntryStore
import IPFSLog.Hashed (Hashed(..))
import IPFSLog.Store.IPFSEntryStore (IPFSEntryStore, mkIPFSEntryStore)
import Ipfs.Api.Commands.Object.Internal (LinkObject)
import Ipfs.Api.Commands.PubSub.Pub (run) as Ipfs.Api.Commands.PubSub.Pub
import Ipfs.Api.Commands.PubSub.Sub (mkArgs, run) as Ipfs.Api.Commands.PubSub.Sub
import Ipfs.Encoded (Base58Encoded)
import Ipfs.Multihash (Multihash)
import LogMetadata (LogMetadata)
import Node.Buffer (fromString, toString) as Buffer
import Node.Encoding (Encoding(..))
import Prelude (class Show, Unit, Void, bind, discard, identity, pure, show, when, (#), ($), (<#>), (<$>), (<<<), (<>), (<@>), (=<<), (>>=))
import Test.QuickCheck.Gen (runGen, shuffle)

type PubSubLogUpdateMessage
  = { newHead   :: LinkObject
    , replacing :: Array (Base58Encoded Multihash)
    }

newtype DummyLogID
  = DummyLogID Void

newtype DummyClockID
  = DummyClockID Void

newtype DummyPayload
  = DummyPayload Void

type LogInfo
  = { heads      :: Array LinkObject
    , entryStore :: IPFSEntryStore DummyLogID DummyClockID DummyPayload
    }

newtype Agent =
  Agent
    { agentId  :: UUID
    , logInfos :: Map UUID LogInfo
    }

instance showAgent :: Show Agent where
  show (Agent a) =
    let
      logInfos
        :: Array
             (Tuple
               UUID
               { heads      :: Array LinkObject
               , entryStore :: IPFSEntryStore DummyLogID DummyClockID DummyPayload
               }
             )
      logInfos =
        toUnfoldable a.logInfos
      showLogInfos =
        logInfos <#> (\(uuid /\ logInfo) ->
            "(log-uuid: " <> show uuid <> ", heads:" <> show logInfo.heads <> ") ")
    in "agent-UUID: " <> show a.agentId <> ", " <> fromCharArray showLogInfos

createArbitraryAgents
  :: LogMetadata
  -> Int
  -> Int
  -> IpfsGenContext (Array Agent)
createArbitraryAgents (logMetadata) numAgents logsPerAgent = do
  {ipfs, refGenState} <- ask
  for (range 1 numAgents) $ \_ -> do
    agentId <- liftEffect genUUID

    -- determine the logs to which this agent subscribes; and create
    -- log-specific state
    genState <- liftEffect $ read refGenState
    let
      shuffledLogs /\ genState' =
        runGen (shuffle logMetadata.logs) genState
      logInfoTuples =
        A.take logsPerAgent shuffledLogs <#> \log ->
          let
            heads =
              [log.link]
            entryStore =
              mkIPFSEntryStore ipfs
          in log.logId /\ {heads, entryStore}
      logInfos =
        fromFoldable logInfoTuples
    liftEffect $ write genState' refGenState -- update generator state

    pure $ Agent {agentId, logInfos}

subscribeAgentsToGroupTopics
  :: Array (Ref Agent)
  -> IpfsGenContext Unit
subscribeAgentsToGroupTopics agentRefs = do
  {ipfs} <- ask
  parTraverse_ <@> agentRefs $ \refAgentState -> do
    agentId /\ logUUIDs <- liftEffect $
      (\(Agent a) -> a.agentId /\ keys a.logInfos) <$> Ref.read refAgentState
    parTraverse_ <@> logUUIDs $ \logUUID -> do
      let
        pubsubTopic =
          show logUUID
        handleLogUpdateMessage msg =
          apathize $ forkAff $ do
            msgData <- liftEffect $ Buffer.toString UTF8 msg."data"
            for_ (jsonParser msgData >>= decodeJson) $
              \(received :: PubSubLogUpdateMessage) -> do
                Agent agent <- liftEffect $ Ref.read refAgentState
                for_ (Map.lookup logUUID agent.logInfos) $ \logInfo -> do
                  maybeFetched :: Maybe (Hashed (Base58Encoded Multihash) E) <-
                    runNoLoggingT $
                      EntryStore.getEntry
                        received.newHead.hash
                        logInfo.entryStore
                  when (isJust maybeFetched) $ do
                    Agent agentState <- liftEffect $
                      Ref.read refAgentState
                    for_ (Map.lookup logUUID agentState.logInfos) $
                      \latestLogInfo -> do
                        let
                          newHeads =
                            received.newHead A.:
                              filter
                                (\lo -> lo.hash `notElem` received.replacing)
                                latestLogInfo.heads
                          logInfo' =
                            latestLogInfo {heads = newHeads}
                          logInfos' =
                            Map.insert logUUID logInfo' agentState.logInfos
                          agentState' =
                            Agent $ agentState {logInfos = logInfos'}
                        liftEffect $ Ref.write agentState' refAgentState

      L.d $ "sub: agent-UUID: " <> show agentId <> " -> " <> show logUUID
      lift $
        expectRight =<< runExceptT do
          Ipfs.Api.Commands.PubSub.Sub.run
            (Ipfs.Api.Commands.PubSub.Sub.mkArgs {topic: pubsubTopic})
            handleLogUpdateMessage
            ipfs

generateAgentMessages
  :: Array (Ref Agent)
  -> IpfsGenContext Unit
generateAgentMessages agentRefs = do
  {ipfs, refGenState, delayGenerator} <- ask
  parTraverse_ <@> agentRefs $ \refAgentState -> do
    logUUIDs <- liftEffect $
      (keys <<< (\(Agent a) -> a.logInfos)) <$> Ref.read refAgentState

    let
      -- helper to pick a delay and log
      pickParameters = do
        genState <- liftEffect $ read refGenState
        let
          nonEmptyList =
            maybe (NEL.singleton emptyUUID) identity (NEL.fromList logUUIDs)
          logUUIDChooser =
            elements nonEmptyList
          logUUID /\ genState' = runGen logUUIDChooser genState
          delayMS /\ genState'' =
            runGen delayGenerator genState'
        liftEffect $ write genState'' refGenState -- update generator state
        pure $ delayMS /\ logUUID

      -- helper to generate an arbitrary message for a particular log
      generateEntryAndPublish logUUID = do
        Agent agent <- liftEffect $ Ref.read refAgentState
        traceM $ "agent " <> (shorten <<< show) agent.agentId <>
          " adding entry to logUUID " <> (shorten <<< show) logUUID

        for_ (Map.lookup logUUID agent.logInfos) $ \logInfo -> do
          genState <- liftEffect $ read refGenState
          let
            genE =
              arbitraryEntry logInfo.heads
            newE /\ genState' =
              runGen genE genState
          liftEffect $ write genState' refGenState -- update generator state

          Hashed newDAGEntry :: Hashed (Base58Encoded Multihash) E <-
            lift $ EntryStore.createEntry newE logInfo.entryStore

          let
            topic =
              show logUUID
            info =
               { newHead: { name: "parent"
                          , hash: newDAGEntry.hash
                          , size: newDAGEntry.size
                          }
               , replacing: _.hash <$> logInfo.heads
               }
            publishMsg =
              (stringify <<< encodeJson) info
          lift $
            expectRight =<< runExceptT do
              dat <- liftEffect $ Buffer.fromString publishMsg UTF8
              Ipfs.Api.Commands.PubSub.Pub.run
                { topic, datas: NE.singleton dat }
                ipfs

    forever $ do
      delayMS /\ logUUID <- pickParameters
      liftAff $ delay $ delayMS # Milliseconds

      generateEntryAndPublish logUUID
