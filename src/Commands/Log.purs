module Command.Log where

{-
import Prelude

import Common (Context)
import Control.Monad.Except.Trans (runExceptT)
import Control.Monad.MonadLogging (d) as L
import Control.Monad.Reader.Class (ask)
import Control.Monad.Trans.Class (lift)
import Data.Argonaut (class DecodeJson, decodeJson, jsonParser, (.:))
import Data.Array (head)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Effect.Class (liftEffect)
import Effect.Class.Console (log)
import Ipfs.Api.Commands.Name.Resolve (mkArgsUsingKey, run) as Name.Resolve
import Ipfs.Api.Commands.Object.Get (run) as Ipfs.Api.Commands.Object.Get
import Ipfs.Encoded (Base58Encoded) as Ipfs
import Ipfs.Encoded (unsafeFromString)
import Ipfs.Multihash (Multihash) as Ipfs
import Node.Process (exit) as Process
import Text.Wrap (dedent)

usage :: String
usage =
  dedent """
Usage: <progname> log command

Commands:
  list
    show log ids with associated root hashes
  add <n>
    add <n> log ids and show associated root hashes
"""

newtype LogIdsMetadata =
  LogIdsMetadata
    { logs ::
        Array
          { groupId :: String
          , hash    :: Ipfs.Base58Encoded Ipfs.Multihash
          }
    }

derive newtype instance showLogIdsMetadata :: Show LogIdsMetadata

instance decodeJsonLogIdsMetadata :: DecodeJson LogIdsMetadata where
  decodeJson json = decodeJson json >>= \o -> ado
      logs <- o .: "logs"
    in LogIdsMetadata { logs }

logIdsMetadata :: Context (Maybe LogIdsMetadata)
logIdsMetadata = do
  {ipfs, logIdsKey} <- ask
  lift $ do
    runExceptT
      (Name.Resolve.run <@> ipfs $
        Name.Resolve.mkArgsUsingKey logIdsKey) >>=
      case _ of
        Left _       -> do
          L.d $ "logIdsMetadata: ipns: failed to resolve key log-ids"
          pure Nothing
        Right {path: str} -> do
          runExceptT
            (Ipfs.Api.Commands.Object.Get.run <@> ipfs $
              {hash: unsafeFromString str}) >>=
            case _ of
              Left _ -> do
                L.d $ "logIdsMetadata: ipfs: failed to fetch metadata object"
                pure Nothing
              Right v ->
                case jsonParser v."data" >>= decodeJson of
                  Left err -> do
                    L.d $ "logIdsMetadata: failed to parse metadata object: " <> err
                    pure Nothing
                  Right o ->
                    pure $ Just o

main :: Context Unit
main = do
  {args} <- ask
  case head args of
    Just "list" -> do
      maybeMeta <- logIdsMetadata
      liftEffect $
        case maybeMeta of
          Just m ->
            log $ "metadata-object: " <> show m
          _ ->
            pure unit

    Just "add" ->
      pure unit

    _ ->
      liftEffect $ do
        log usage
        Process.exit 1
-}
