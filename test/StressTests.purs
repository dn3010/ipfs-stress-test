module StressTests
  ( initialAccess
  , stressPubsub
  , createLinearDAG
  , createBinaryTrees
  ) where

import Prelude

import Control.Monad.Error.Class (throwError, try)
import Control.Monad.Except.Trans (runExceptT)
import Control.Monad.Fork.Class (fork)
import Control.Monad.MonadLogging (LoggingT, runNoLoggingT, runStdoutLoggingT)
import Control.Monad.Reader.Trans (runReaderT)
import Control.Monad.Rec.Class (forever)
import Control.Parallel (parOneOf, parTraverse, parTraverse_)
import Dag (DAG(..), createArbitraryDAGWithDepth, concurrentlyAccessArbitraryObjects)
import Data.Array (index, length, range, sort, (:))
import Data.Array.NonEmpty (toArray)
import Data.DateTime.Instant (unInstant)
import Data.Either (Either(..))
import Data.Foldable (for_)
import Data.Int (fromString, toNumber)
import Data.List.Lazy (replicateM)
import Data.Maybe (Maybe(..))
import Data.Newtype (unwrap)
import Data.NonEmpty ((:|))
import Data.Number.Format (toStringWith, fixed)
import Data.String.Regex (match)
import Data.String.Regex.Flags (noFlags)
import Data.String.Regex.Unsafe (unsafeRegex)
import Data.Time.Duration (Milliseconds(..))
import Data.Tuple (Tuple)
import Data.Tuple.Nested ((/\))
import Effect.Aff (Aff, delay)
import Effect.Aff (error) as Error
import Effect.Aff.AVar (AVar, empty, kill, put, take) as AVar
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Class (liftEffect)
import Effect.Class.Console (log)
import Effect.Now (now) as Date
import Effect.Ref (new)
import Ipfs.Api.Client (Client) as Ipfs.Api.Client
import Ipfs.Api.Commands.Id (run) as Ipfs.Api.Commands.Id
import Ipfs.Api.Commands.Object.Get (run) as Ipfs.Api.Commands.Object.Get
import Ipfs.Api.Commands.Object.Put (ObjectUpload(..))
import Ipfs.Api.Commands.Object.Put (mkArgs, run') as Ipfs.Api.Commands.Object.Put
import Ipfs.Api.Commands.Object.Stat (run) as Ipfs.Api.Commands.Object.Stat
import Ipfs.Api.Commands.PubSub.Pub (run) as Ipfs.Api.Commands.PubSub.Pub
import Ipfs.Api.Commands.PubSub.Sub (PubsubMessage)
import Ipfs.Api.Commands.PubSub.Sub (mkArgs, run) as Ipfs.Api.Commands.PubSub.Sub
import Ipfs.Encoded (Base58Encoded) as Ipfs
import Ipfs.Encoded (unsafeFromString)
import Ipfs.Multihash (Multihash) as Ipfs
import Node.Buffer (fromString, toString) as Buffer
import Node.Encoding (Encoding(..))
import Random.LCG (randomSeed)
import Test.Spec (Spec, describe, it)
import Test.Spec.Assertions (shouldEqual)

trivialString :: String
trivialString = "foooobar!"

hashOfTrivialString :: {hash :: Ipfs.Base58Encoded Ipfs.Multihash}
hashOfTrivialString =
  {hash: unsafeFromString "QmUpsX5PgeumQN9cyn38XPkh8ApaoUQyHTtoJcxmgNxn3f"}

initialAccess :: Ipfs.Api.Client.Client -> Spec Unit
initialAccess ipfs =
  describe "ipfs-access" $ do
    testId
    putSingleTrivialObject
    repeatedlyPutSingleTrivialObject
    statTrivialObject
    fetchTrivialObject
    attemptPubsub
  where
  testId :: Spec Unit
  testId =
    it "attempts API id" do
      runStdoutLoggingT (const identity) do
        void $
          expectRight =<< runExceptT do
            Ipfs.Api.Commands.Id.run <@> ipfs $ {peerId: Nothing}

  putSingleTrivialObject :: Spec Unit
  putSingleTrivialObject =
    it "trivial object put" do
      simpleObjectPut

  repeatedlyPutSingleTrivialObject :: Spec Unit
  repeatedlyPutSingleTrivialObject =
    it "repeated trivial object put" do
    void $ replicateM 4 $ simpleObjectPut

  simpleObjectPut :: Aff Unit
  simpleObjectPut =
    runNoLoggingT do
      h :| _ <- expectRight =<< runExceptT do
        Ipfs.Api.Commands.Object.Put.run' <@> ipfs $
          (Ipfs.Api.Commands.Object.Put.mkArgs
            { uploads:
                FromMemoryJson
                  { links: []
                  , "data": trivialString
                  } :| []
            })
      liftAff $ h `shouldEqual` hashOfTrivialString

  statTrivialObject :: Spec Unit
  statTrivialObject =
    it "fetches metadata about trivial object" do
      runNoLoggingT do
        h <- expectRight =<< runExceptT do
          Ipfs.Api.Commands.Object.Stat.run <@> ipfs $ hashOfTrivialString
        liftAff $ do
          h.hash `shouldEqual` hashOfTrivialString.hash
          h.numLinks `shouldEqual` 0

  fetchTrivialObject :: Spec Unit
  fetchTrivialObject =
    it "fetches trivial object" do
      runNoLoggingT do
        t <- expectRight =<< runExceptT do
          Ipfs.Api.Commands.Object.Get.run <@> ipfs $ hashOfTrivialString
        liftAff $ do
          t.data `shouldEqual` trivialString
          t.links `shouldEqual` []

  attemptPubsub :: Spec Unit
  attemptPubsub =
    it "tests pubsub" do
      runNoLoggingT do
        v <- liftAff AVar.empty
        let
          topic =
            "foobar"
          msg1 =
            "hi (1)"
          msg2 =
            "hi (2)"
        void $ parOneOf $ try <$>
          [ do
            expectRight =<< runExceptT do
              Ipfs.Api.Commands.PubSub.Sub.run
                (Ipfs.Api.Commands.PubSub.Sub.mkArgs {
                  topic
                }) (AVar.put <@> v) ipfs
          , do
            liftAff $ delay $ 50.0 # Milliseconds -- ensure 'sub' arrives first
            expectRight =<< runExceptT do
              dat <- liftEffect $ Buffer.fromString msg1 UTF8
              Ipfs.Api.Commands.PubSub.Pub.run
                { topic, datas: dat :| [] }
                ipfs
            void $ expectRight =<< runExceptT do
              dat <- liftEffect $ Buffer.fromString msg2 UTF8
              Ipfs.Api.Commands.PubSub.Pub.run
                { topic, datas: dat :| [] }
                ipfs
            x <- liftEffect <<< Buffer.toString UTF8 <<< _."data" =<< liftAff (AVar.take v)
            liftAff $ x `shouldEqual` msg1
            y <- liftEffect <<< Buffer.toString UTF8 <<< _."data" =<< liftAff (AVar.take v)
            liftAff $ y `shouldEqual` msg2
          ]

createLinearDAG :: Ipfs.Api.Client.Client -> Spec Unit
createLinearDAG ipfs =
  it "creates an arbitrary linear DAG" do
    refGenState <- liftEffect $ do
      seed <- randomSeed
      let
        genState =
          {newSeed: seed, size: 100}
      new genState

    let
      makeChain =
        {linear: 1.0, diamond: 0.0, fork: 0.0}
    for_ (range 1 15) $ \n -> do
      runStdoutLoggingT (const identity) $ do
        latency /\ dag@(DAG (h:|r)) <- measureLatency $
          runReaderT <@> {ipfs, refGenState} $
            createArbitraryDAGWithDepth makeChain n
        let
          nEntries =
            length r + 1
        log $ "created chain of " <> show nEntries <> " entries: " <>
          show latency <> " ms; " <>
          toStringWith (fixed 1) (latency / toNumber nEntries) <> " ms/op"

createBinaryTrees :: Ipfs.Api.Client.Client -> Spec Unit
createBinaryTrees ipfs =
  it "creates an arbitrary binary tree" do
    refGenState <- liftEffect $ do
      seed <- randomSeed
      let
        genState =
          {newSeed: seed, size: 100}
      new genState

    let
      makeFork =
        {linear: 0.0, diamond: 0.0, fork: 1.0}
      depth =
        12
    {--for_ (range 12 12) $ \n -> do--}
    runStdoutLoggingT (const identity) $ do
      runReaderT <@> {ipfs, refGenState} $ do
        createLatency /\ dag@(DAG (h:|r)) <- measureLatency $
          createArbitraryDAGWithDepth makeFork depth
        let
          nEntries =
            length r + 1
        log $ "created complete binary tree of " <> show nEntries <> " entries: " <>
          show createLatency <> " ms; " <>
          toStringWith (fixed 1) (createLatency / toNumber nEntries) <> " ms/op"

        forever $ do
          accessLatency /\ nAccessed <- measureLatency $
            concurrentlyAccessArbitraryObjects 1000 (h : r)
          log $ "access latency for " <> show nAccessed <> " random objects: " <>
            show accessLatency <> " ms; " <>
            toStringWith (fixed 1) (accessLatency / toNumber nAccessed) <>
              " ms/op"

stressPubsub :: Ipfs.Api.Client.Client -> Spec Unit
stressPubsub ipfs =
  describe "stress pubsub" do
    let
      topic =
        "somechannel"
      msg n =
        "hi (" <> show n <> ")"

      publishMessage :: Int -> LoggingT Aff Unit
      publishMessage n =
        expectRight =<< runExceptT do
          dat <- liftEffect $ Buffer.fromString (msg n) UTF8
          Ipfs.Api.Commands.PubSub.Pub.run
            { topic, datas: dat :| [] }
            ipfs

      subscribeToMessages :: AVar.AVar PubsubMessage -> LoggingT Aff Unit
      subscribeToMessages q =
        expectRight =<< runExceptT do
          Ipfs.Api.Commands.PubSub.Sub.run
            (Ipfs.Api.Commands.PubSub.Sub.mkArgs {
              topic
            }) (\x -> void $ fork $ AVar.put <@> q $ x)
            ipfs

      validateMessage :: AVar.AVar PubsubMessage -> Int -> LoggingT Aff Unit
      validateMessage q n = do
        s <- liftEffect <<< Buffer.toString UTF8 <<< _."data" =<< liftAff (AVar.take q)
        liftAff $ s `shouldEqual` (msg n)

      ensureMessageReception :: AVar.AVar PubsubMessage -> LoggingT Aff Int
      ensureMessageReception q = do
        m <- liftEffect <<< Buffer.toString UTF8 <<< _."data" =<< liftAff (AVar.take q)
        let
          parse = -- parse msg-id from the message
            do nea <- match (unsafeRegex "^hi \\((\\d+)\\)$" noFlags) m
               justMatch <- index (toArray nea) 1
               justMatch >>= fromString
        case parse of
          Just n -> pure n
          _      -> pure 0  -- illegal message ID

    it "publishes and receives messages sequentially" do
      runNoLoggingT $ do
        v :: AVar.AVar PubsubMessage <- liftAff AVar.empty
        ret <- parOneOf $ try <$>
          [ subscribeToMessages v
          , do
              liftAff $ delay $ 50.0 # Milliseconds -- ensure 'sub' arrives first
              let
                cNumSequentialMessages =
                  20
              for_ (range 1 cNumSequentialMessages) $ \n -> do
                publishMessage n
                validateMessage v n
          ]
        case ret of
          Left e -> liftAff $ throwError e
          _      -> liftAff $ AVar.kill (Error.error "finished") v

    it "batch-publishes messages and then batch-receives them" do
      runNoLoggingT $ do
        v :: AVar.AVar PubsubMessage <- liftAff AVar.empty
        ret <- parOneOf $ try <$>
          [ subscribeToMessages v
          , do
              liftAff $ delay $ 50.0 # Milliseconds -- ensure 'sub' arrives first
              let
                cNumSequentialMessages =
                  20
              for_ (range 1 cNumSequentialMessages) publishMessage
              for_ (range 1 cNumSequentialMessages) (validateMessage v)
          ]
        case ret of
          Left e -> liftAff $ throwError e
          _      -> liftAff $ AVar.kill (Error.error "finished") v

    it "publishes and receives messages in parallel" do
      runNoLoggingT $ do
        v :: AVar.AVar PubsubMessage <- liftAff AVar.empty
        ret <- parOneOf $ try <$>
          [ subscribeToMessages v
          , do
              liftAff $ delay $ 50.0 # Milliseconds -- ensure 'sub' arrives first
              let
                cNumParallelMessages =
                  1000
              parTraverse_ <@> (range 1 cNumParallelMessages) $ publishMessage
              ns <- parTraverse <@> (range 1 cNumParallelMessages) $
                      const (ensureMessageReception v)
              liftAff $ sort ns `shouldEqual` (range 1 cNumParallelMessages)
          ]
        case ret of
          Left e -> liftAff $ throwError e
          _      -> liftAff $ AVar.kill (Error.error "finished") v

expectRight :: ∀ m a b. MonadAff m => Either a b -> m b
expectRight (Left e) = liftAff $ throwError (Error.error "Expected Right")
expectRight (Right v) = pure v

measureLatency
  :: ∀ m a
   . MonadAff m
  => m a
  -> m (Tuple Number a)
measureLatency action = do
  now <- liftEffect Date.now
  r <- action
  now' <- liftEffect Date.now
  let
    duration :: Number
    duration =
      unwrap (unInstant now') - unwrap (unInstant now)
  pure $ duration /\ r
