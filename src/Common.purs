module Common where

import Prelude

import Control.Monad.Error.Class (throwError)
import Control.Monad.MonadLogging (LoggingT)
import Control.Monad.Reader.Trans (ReaderT)
import Data.BigInt (toNumber)
import Data.Either (Either(..))
import Data.Int (round)
import Data.String.CodeUnits (length, take, takeRight)
import Effect.Aff (Aff)
import Effect.Aff (error) as Error
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Ref (Ref)
import Ipfs.Api.Client (Client) as Ipfs.Api.Client
import Ipfs.Encoded (Base58Encoded) as Ipfs
import Ipfs.Multihash (Multihash) as Ipfs
import Ipfs.Size (Size) as Ipfs.Size
import Test.QuickCheck.Gen (Gen, GenState)
import Unsafe.Coerce (unsafeCoerce)

type IpfsGenContext a
  = ReaderT
      { ipfs           :: Ipfs.Api.Client.Client
      , refGenState    :: Ref GenState
      , delayGenerator :: Gen Number
      }
      (LoggingT Aff)
      a

newtype LogIdsMetadata =
  LogIdsMetadata
    { logs ::
        Array
          { groupId :: String
          , hash    :: Ipfs.Base58Encoded Ipfs.Multihash
          }
    }

expectRight :: ∀ m a b. MonadAff m => Either a b -> m b
expectRight (Left e) = liftAff $ throwError (Error.error "Expected Right")
expectRight (Right v) = pure v

bigIntToInt :: Ipfs.Size.Size -> Int
bigIntToInt = round <<< toNumber <<< unsafeCoerce

shorten :: String -> String
shorten s
  | length s > 11 = (take 4 s) <> ".." <> (takeRight 4 s)
  | otherwise = s
