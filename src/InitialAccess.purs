module InitialAccess where

import Prelude

import Control.Monad.Error.Class (throwError, try)
import Control.Monad.Except.Trans (runExceptT)
import Control.Monad.MonadLogging (runNoLoggingT, runStdoutLoggingT)
import Control.Parallel (parOneOf)
import Data.Either (Either(..))
import Data.List.Lazy (replicateM)
import Data.Maybe (Maybe(..))
import Data.NonEmpty ((:|))
import Data.Time.Duration (Milliseconds(..))
import Effect.Aff (Aff, delay)
import Effect.Aff (error) as Error
import Effect.Aff.AVar (empty, put, take) as AVar
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Class (liftEffect)
import Ipfs.Api.Client (Client) as Ipfs.Api.Client
import Ipfs.Api.Commands.Id (run) as Ipfs.Api.Commands.Id
import Ipfs.Api.Commands.Object.Get (run) as Ipfs.Api.Commands.Object.Get
import Ipfs.Api.Commands.Object.Put (ObjectUpload(..)) as ObjectUpload
import Ipfs.Api.Commands.Object.Put (mkArgs, run') as Ipfs.Api.Commands.Object.Put
import Ipfs.Api.Commands.Object.Stat (run) as Ipfs.Api.Commands.Object.Stat
import Ipfs.Api.Commands.PubSub.Pub (run) as Ipfs.Api.Commands.PubSub.Pub
import Ipfs.Api.Commands.PubSub.Sub (mkArgs, run) as Ipfs.Api.Commands.PubSub.Sub
import Ipfs.Encoded (Base58Encoded) as Ipfs
import Ipfs.Encoded (unsafeFromString)
import Ipfs.Multihash (Multihash) as Ipfs
import Node.Buffer (fromString, toString) as Buffer
import Node.Encoding (Encoding(..))
import Test.Spec (Spec, describe, it)
import Test.Spec.Assertions (shouldEqual)

trivialString :: String
trivialString = "foooobar!"

hashOfTrivialString :: {hash :: Ipfs.Base58Encoded Ipfs.Multihash}
hashOfTrivialString =
  {hash: unsafeFromString "QmUpsX5PgeumQN9cyn38XPkh8ApaoUQyHTtoJcxmgNxn3f"}

spec :: Ipfs.Api.Client.Client -> Spec Unit
spec ipfs =
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
                ObjectUpload.FromMemoryJson
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

expectRight :: ∀ m a b. MonadAff m => Either a b -> m b
expectRight (Left e) = liftAff $ throwError (Error.error "Expected Right")
expectRight (Right v) = pure v
