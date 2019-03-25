module Entry
  ( E(..)
  , arbitraryEntry
  , entryToSize
  , entryToDataSize
  )
  where

import Common (bigIntToInt)
import Control.MonadZero (guard)
import Data.Argonaut (class EncodeJson, Json, encodeJson, jsonEmptyObject, (:=), (~>))
import Data.Array (catMaybes) as A
import Data.Array (snoc, (:))
import Data.Char.Unicode (toUpper)
import Data.Foldable (sum)
import Data.List (List(..))
import Data.Maybe (Maybe(..))
import Data.NonEmpty ((:|))
import Data.String.CodeUnits (fromCharArray) as String.CodeUnits
import Data.String.CodeUnits (length, toCharArray)
import Data.Tuple.Nested ((/\))
import Ipfs.Api.Commands.Object.Internal (LinkObject)
import Ipfs.Api.Extras.Dag.Rep (class LoadDAGRep, class ToDAGRep, DAGRep(..), LinkRep(..), ReferenceLinkRep(..)) as Ipfs.Api.Extras
import Ipfs.Size (Size) as Ipfs.Size
import Ipfs.Size (unsafeFromInt)
import Prelude (class Eq, class Show, apply, map, pure, show, ($), (+), (<#>), (<$), (<$>), (<<<), (<>), (==))
import Test.QuickCheck.Gen (Gen, chooseInt, elements, frequency, vectorOf)

newtype E =
  E
    { d     :: String
    , links :: Array LinkObject
    }

derive newtype instance eqE :: Eq E
derive newtype instance showE :: Show E

instance encodeJsonE :: EncodeJson E where
  encodeJson (E { links, d }) = do
       "Links"  := links
    ~> "Data"   := d
    ~> jsonEmptyObject

instance toDAGRepE :: Ipfs.Api.Extras.ToDAGRep E Json where
  toDAGRep (E {d, links}) =
    let
      _data =
        encodeJson d
      links' =
        Ipfs.Api.Extras.ReferenceLink <$> links
    in Ipfs.Api.Extras.DAGRep _data links'

instance loadDAGRepE :: Ipfs.Api.Extras.LoadDAGRep E where
  loadDag (Ipfs.Api.Extras.DAGRep { "data": d } links) ipfs =
    let
      parents =
        A.catMaybes $
          links <#> case _ of
            Ipfs.Api.Extras.ReferenceLinkRep link@{ name, size, hash } ->
              link <$ do
                guard $ name == "parent"
    in pure $ Just $ E {d, links: parents}

entryToSize :: E -> Ipfs.Size.Size
entryToSize (E {links, d}) =
  let
    lenD =
      length d
    lenLs =
      sum $ (bigIntToInt <<< _.size) <$> links
  in unsafeFromInt $ lenD + lenLs

entryToDataSize :: E -> Ipfs.Size.Size
entryToDataSize (E {d}) =
  let
    lenD =
      length d
  in unsafeFromInt $ lenD

-- | Generates a single arbitray node/Entry from a given collection of parent
-- | hashes.
arbitraryEntry :: Array LinkObject -> Gen E
arbitraryEntry links =
  let
    numbers =
      '1' :| toCharArray "23456789"
    chars =
      'a' :| toCharArray "abcdefghijklmnopqrstuvwxyz"
    punctuation =
      ',' :| toCharArray ".;  "
    endPunctuation =
      ',' :| toCharArray ".;"
    messageGen = ado
      cap     <- toUpper <$> elements chars
      letters <- vectorOf 16 $
        frequency $
          (0.80 /\ (elements chars))
            :| Cons (0.05 /\ (elements numbers))
                (Cons (0.15 /\ (elements punctuation)) Nil)
      end <- elements endPunctuation
      in cap : snoc letters end
    senderGen = ado
      s <- elements $ "Sun" :| ["Moon", "Earth", "Mars", "Saturn", "Pluto"]
      n <- chooseInt 1 20
      in s <> show n
  in ado
    msg    <- String.CodeUnits.fromCharArray <$> messageGen
    pri    <- chooseInt 0 5
    sender <- senderGen
  in E { links
       , d: "{msg: " <> show msg <> ", sender: " <> show sender
             <> ", priority: " <> show pri <> "}"
       }

-- TODO: remove the following if not used
{---- | Adapts array of (Hashed k v) to arbitraryEntry taking LinkOjects--}
{--arbitraryEntry' :: Array (Hashed (Base58Encoded Multihash) E) -> Gen E--}
{--arbitraryEntry' hashLinks =--}
  {--let--}
    {--linkObjects =--}
      {--hashLinks <#> \(Hashed hlink) ->--}
        {--{name: "parent", hash: hlink.hash, size: hlink.size}--}
  {--in arbitraryEntry linkObjects--}
