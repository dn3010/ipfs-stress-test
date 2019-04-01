module Agent
  ( Agent
  , createArbitraryAgents
  , subscribeAgentsToGroupTopics
  , generateAgentMessages
  , LogID
  , AgentID
  , log2AgentsLookupTable
  , PubsubRecord
  , RefPubsubRecords
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
import Data.Array (cons, length, snoc, sort, take, uncons, (:)) as A
import Data.Array (filter, findIndex, range, unsafeIndex, (:))
import Data.DateTime.Instant (Instant, instant, unInstant)
import Data.Foldable (foldr, for_, notElem)
import Data.List (toUnfoldable) as L
import Data.List.NonEmpty (fromList, singleton) as NEL
import Data.Map.Internal (Map, alter, fromFoldable, keys, toUnfoldable)
import Data.Map.Internal (empty, insert, lookup) as Map
import Data.Maybe (Maybe(..), fromMaybe, isJust, maybe)
import Data.NonEmpty (singleton) as NE
import Data.Ord (lessThan)
import Data.String.Utils (fromCharArray)
import Data.Time.Duration (Milliseconds(..))
import Data.Traversable (for)
import Data.Tuple (Tuple)
import Data.Tuple.Nested ((/\))
import Data.UUID (UUID, emptyUUID, genUUID)
import Effect (Effect)
import Effect.Aff (Aff, apathize, delay, forkAff)
import Effect.Aff.Class (liftAff)
import Effect.Class (liftEffect)
import Effect.Class.Console (log)
import Effect.Now (now)
import Effect.Ref (Ref, modify_, read, write)
import Effect.Ref (modify_, new, read, write) as Ref
import Entry (E, arbitraryEntry)
import IPFSLog.EntryStore.Class (createEntry, getEntry) as EntryStore
import IPFSLog.Hashed (Hashed(..))
import IPFSLog.Store.IPFSEntryStore (IPFSEntryStore, mkIPFSEntryStore)
import Ipfs.Api.Client (Client) as Ipfs.Api.Client
import Ipfs.Api.Commands.Object.Internal (LinkObject)
import Ipfs.Api.Commands.PubSub.Pub (run) as Ipfs.Api.Commands.PubSub.Pub
import Ipfs.Api.Commands.PubSub.Sub (PubsubMessage)
import Ipfs.Api.Commands.PubSub.Sub (mkArgs, run) as Ipfs.Api.Commands.PubSub.Sub
import Ipfs.Encoded (Base58Encoded)
import Ipfs.Multihash (Multihash)
import LogMetadata (LogMetadata)
import Node.Buffer (fromString, toString) as Buffer
import Node.Encoding (Encoding(..))
import Node.Process (exit)
import Partial.Unsafe (unsafePartial)
import Prelude (class Eq, class Show, Unit, Void, bind, discard, identity, pure, show, when, (#), ($), (-), (/=), (<#>), (<$>), (<<<), (<>), (<@>), (=<<), (==), (>>=))
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
  = { heads :: Array LinkObject
    }

type AgentID
  = UUID

type LogID
  = UUID

newtype Agent =
  Agent
    { agentID    :: AgentID
    , ipfsClient :: Ipfs.Api.Client.Client
    , entryStore :: IPFSEntryStore DummyLogID DummyClockID DummyPayload
    , logInfos   :: Map LogID LogInfo
    }

newtype PubsubRecord =
  PubsubRecord
    { logID      :: LogID
    , hash       :: Base58Encoded Multihash
    , checkAt    :: Ref Instant
    , receivedBy :: Ref (Array AgentID)
    }

instance eqPubsubRecord :: Eq PubsubRecord where
  eq (PubsubRecord r1) (PubsubRecord r2) =
    r1.hash == r2.hash

type RefPubsubRecords =
  Ref (Array PubsubRecord)

instance showAgent :: Show Agent where
  show (Agent a) =
    let
      logInfos
        :: Array
             (Tuple
               LogID
               { heads :: Array LinkObject
               }
             )
      logInfos =
        toUnfoldable a.logInfos
      showLogInfos =
        logInfos <#> (\(uuid /\ logInfo) ->
            "(log-uuid: " <> show uuid <> ", heads:" <> show logInfo.heads <> ") ")
    in "agent-UUID: " <> show a.agentID <> ", " <> fromCharArray showLogInfos

createArbitraryAgents
  :: LogMetadata
  -> Int
  -> Int
  -> IpfsGenContext (Array Agent)
createArbitraryAgents (logMetadata) numAgents logsPerAgent = do
  {refGenState, ipfsDestChooser} <- ask
  for (range 1 numAgents) $ \_ -> do
    agentID <- liftEffect genUUID

    -- determine the logs to which this agent subscribes; and create
    -- log-specific state
    genState <- liftEffect $ read refGenState
    let
      ipfsClient /\ genState' =
        runGen ipfsDestChooser genState
      entryStore =
        mkIPFSEntryStore ipfsClient
      shuffledLogs /\ genState'' =
        runGen (shuffle logMetadata.logs) genState'
      logInfoTuples =
        A.take logsPerAgent shuffledLogs <#> \log ->
          let
            heads =
              [log.link]
          in log.logId /\ {heads}
      logInfos =
        fromFoldable logInfoTuples
    liftEffect $ write genState'' refGenState -- update generator state

    pure $ Agent {agentID, ipfsClient, entryStore, logInfos}

subscribeAgentsToGroupTopics
  :: Array (Ref Agent)
  -> RefPubsubRecords
  -> IpfsGenContext Unit
subscribeAgentsToGroupTopics agentRefs refPubsub = do
  {refGenState, ipfsDestChooser} <- ask
  parTraverse_ <@> agentRefs $ \refAgentState -> do
    agentID /\ ipfsClient /\ logUUIDs <- liftEffect $
      (\(Agent a) -> a.agentID /\ a.ipfsClient /\ keys a.logInfos) <$>
        Ref.read refAgentState
    parTraverse_ <@> logUUIDs $ \logUUID -> do
      let
        pubsubTopic =
          show logUUID

      L.d $ "sub: agent-UUID: " <> show agentID <> " -> " <> show logUUID
      lift $
        expectRight =<< runExceptT do
          Ipfs.Api.Commands.PubSub.Sub.run
            (Ipfs.Api.Commands.PubSub.Sub.mkArgs {topic: pubsubTopic})
            (handleLogUpdateMessage refAgentState refPubsub logUUID)
            ipfsClient

generateAgentMessages
  :: Array (Ref Agent)
  -> RefPubsubRecords
  -> Map LogID (Array AgentID)
  -> IpfsGenContext Unit
generateAgentMessages agentRefs refPubsub log2AgentLookupTable = do
  {refGenState, delayChooser} <- ask
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
            runGen delayChooser genState'
        liftEffect $ write genState'' refGenState -- update generator state
        pure $ delayMS /\ logUUID

    forever $ do
      delayMS /\ logUUID <- pickParameters
      liftAff $ delay $ delayMS # Milliseconds

      generateEntryAndPublish refAgentState logUUID refPubsub
      liftEffect $ checkPubsubRecords refPubsub log2AgentLookupTable

handleLogUpdateMessage
  :: Ref Agent
  -> RefPubsubRecords
  -> LogID
  -> PubsubMessage
  -> Aff Unit
handleLogUpdateMessage refAgentState refPubsub logUUID msg =
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
                agent.entryStore
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

          liftEffect $ do
            records <- Ref.read refPubsub
            for_
              (findIndex
                (\(PubsubRecord e) -> e.hash == received.newHead.hash)
                records
              ) $ \i -> do
                let
                  PubsubRecord record =
                    unsafePartial $ records `unsafeIndex` i
                Ref.modify_ <@> record.receivedBy $ A.cons agent.agentID

-- helper to generate an arbitrary message for a particular log
generateEntryAndPublish
  :: Ref Agent
  -> LogID
  -> RefPubsubRecords
  -> IpfsGenContext Unit
generateEntryAndPublish refAgent logUUID refPubsub = do
  {refGenState} <- ask
  Agent agent <- liftEffect $ Ref.read refAgent

  for_ (Map.lookup logUUID agent.logInfos) $ \logInfo -> do
    genState <- liftEffect $ read refGenState
    let
      genE =
        arbitraryEntry logInfo.heads
      newE /\ genState' =
        runGen genE genState
    liftEffect $ write genState' refGenState -- update generator state

    Hashed newDAGEntry :: Hashed (Base58Encoded Multihash) E <-
      lift $ EntryStore.createEntry newE agent.entryStore

    -- append an initial pubsub record corresponding to this publish
    liftEffect $ do
      t <- now
      checkAt <- Ref.new $
        fromMaybe t $ instant $ (unInstant t) <> (10000.0 # Milliseconds)
      receivedBy <- Ref.new []
      modify_ <@> refPubsub $
        A.snoc <@>
          PubsubRecord
            { logID: logUUID
            , hash: newDAGEntry.hash
            , checkAt
            , receivedBy
            }

    -- publish on the log's topic
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
          agent.ipfsClient

checkPubsubRecords
  :: RefPubsubRecords
  -> Map LogID (Array AgentID)
  -> Effect Unit
checkPubsubRecords refPubsub lookupTable = do
  records  <- Ref.read refPubsub
  records' <- go records
  when (records /= records') $ do
    log $ "gc finished with " <> show (A.length records') <> " pending pubsub records"
    Ref.write records' refPubsub
  where
    go :: Array PubsubRecord -> Effect (Array PubsubRecord)
    go rs = case A.uncons rs of
      Nothing -> pure []
      Just {head: PubsubRecord r, tail: rest} -> do
        case Map.lookup r.logID lookupTable of
          Nothing -> do
            log "failed to find log entry in lookupTable"
            exit 1
          Just allAgents -> do
            expiry <- Ref.read r.checkAt
            t <- now
            if (t `lessThan` expiry)
              then pure rs -- stop processing further records, since
                           -- they should be sorted according to time
              else do
                receivedBy <- Ref.read r.receivedBy
                when ((A.sort receivedBy) /= (A.sort allAgents)) $ do
                  log $ "!!!!! missed subs for record " <>
                    shorten (show r.hash) <> "; recvd: " <>
                    show ((shorten <<< show) <$> A.sort receivedBy) <>
                    "; all: " <> show ((shorten <<< show) <$> allAgents) <>
                    "; missing subs: " <>
                    show (A.length allAgents - A.length receivedBy)
                go rest -- drop r

log2AgentsLookupTable :: Array Agent -> Map LogID (Array AgentID)
log2AgentsLookupTable =
  foldr
    (\agent@(Agent {agentID}) ->
      foldr
        (alter $
          case _ of
            Nothing       -> Just $ [agentID]
            Just agentIds -> Just $ agentID : agentIds
        )
        <@>
        (subscribedGroupsFor agent)
    )
    Map.empty
  where
    subscribedGroupsFor :: Agent -> Array LogID
    subscribedGroupsFor (Agent agent) = L.toUnfoldable $ keys agent.logInfos
