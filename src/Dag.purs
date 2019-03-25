module Dag
  ( createArbitraryDAGWithDepth
  , concurrentlyAccessArbitraryObjects
  , DAG(..)
  , toDot
  , DAGExtensionFrequencies
  ) where

import Common (IpfsGenContext, bigIntToInt, expectRight, shorten)
import Prelude (class Show, bind, discard, pure, ($), (+), (-), (<$>), (<<<), (<>), (<@>), (=<<))
import Control.Monad.Except.Trans (runExceptT)
import Control.Monad.Fork.Class (fork, join)
import Control.Monad.MonadLogging (runNoLoggingT)
import Control.Monad.Reader.Class (ask)
import Control.Monad.Reader.Trans (withReaderT)
import Control.Parallel (parTraverse_)
import Data.Array (length, take) as A
import Data.Array (union, (:))
import Data.Foldable (foldr)
import Data.List.Types (List(..))
import Data.NonEmpty (NonEmpty, (:|))
import Data.Tuple (Tuple)
import Data.Tuple.Nested ((/\))
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Class (liftEffect)
import Effect.Ref (Ref, new, read, write)
import Entry (E(..), arbitraryEntry, entryToSize)
import Ipfs.Api.Client (Client) as Ipfs.Api.Client
import Ipfs.Api.Commands.Object.Get (run) as Ipfs.Api.Commands.Object.Get
import Ipfs.Api.Commands.Object.Internal (LinkObject)
import Ipfs.Api.Commands.Object.Put (ObjectUpload(..))
import Ipfs.Api.Commands.Object.Put (mkArgs, run') as Ipfs.Api.Commands.Object.Put
import Ipfs.Api.Commands.Object.Stat (run) as Ipfs.Api.Commands.Object.Stat
import Ipfs.Encoded (Base58Encoded, toString) as Ipfs
import Ipfs.Multihash (Multihash) as Ipfs
import Ipfs.Size (unsafeFromInt)
import Random.LCG (mkSeed, unSeed)
import Test.QuickCheck.Gen (GenState, frequency, runGen, shuffle)
import Test.Spec.Assertions (shouldEqual)

newtype DAG
  = DAG (NonEmpty Array (Tuple LinkObject E))

derive newtype instance showDAG :: Show DAG

type DAGExtensionFrequencies =
  { linear  :: Number
  , diamond :: Number
  , fork    :: Number
  }

data DAGExtension
  = Linear
  | Diamond
  | Fork

-- add new entry as an IPFS object
addIpfsObject
  :: ∀ m
   . MonadAff m
  => Ipfs.Api.Client.Client
  -> Ref GenState
  -> Array LinkObject
  -> m (Tuple LinkObject E)
addIpfsObject ipfs refGenState parentLinks = do
  genState <- liftEffect $ read refGenState
  let
    entryGen =
      arbitraryEntry parentLinks
    newEntry@(E {links, d}) /\ genState' =
      runGen entryGen genState
    newobj =
      FromMemoryJson {"data": d, links}
  liftEffect $ write genState' refGenState -- update generator state

  h :| _   <- liftAff $ runNoLoggingT $ expectRight =<< runExceptT do
    Ipfs.Api.Commands.Object.Put.run' <@> ipfs $
      (Ipfs.Api.Commands.Object.Put.mkArgs
        { uploads: newobj :| [] })
  pure $ {name: "parent", hash: h.hash, size: entryToSize newEntry} /\ newEntry

-- | Generates an array of arbitrary entries which are internally linked
-- | to form a DAG. The depth parameter controls the depth of the DAG.
createArbitraryDAGWithDepth
  :: DAGExtensionFrequencies
  -> Int
  -> IpfsGenContext DAG
createArbitraryDAGWithDepth _freqs 0 = do
  {ipfs, refGenState} <- ask
  genState            <- liftEffect $ read refGenState
  DAG <<< (_ :| []) <$> addIpfsObject ipfs refGenState [{- no parents -}]

createArbitraryDAGWithDepth f d = do
  {ipfs, refGenState} <- ask
  genState <- liftEffect $ read refGenState

  -- Each of the following generators will produce an arbitrary DAG
  -- represented as an Array of entries. It is guaranteed that the
  -- first element of each Array is the root of the DAG
  let
    extensionGen =
      frequency $
        (f.linear /\ (pure Linear)) :|
          Cons (f.diamond /\ (pure Diamond)) (Cons (f.fork /\ (pure Fork)) Nil)
    extensionChoice /\ genState' =
      runGen extensionGen genState
  liftEffect $ write genState' refGenState -- update generator state
  case extensionChoice of
    Linear -> do
      -- extend base subtree with a single new root (forming a chain)
      DAG (subtreeHead@(subtreeRootLinkObject /\ _) :| rest) <-
        createArbitraryDAGWithDepth f (d - 1)
      linkObjectAndE <- addIpfsObject ipfs refGenState [subtreeRootLinkObject]
      pure $ DAG (linkObjectAndE :| subtreeHead : rest)

    Diamond -> do
        -- extend base subtree with a diamond structure
        DAG (subtreeHead@(subtreeRootLinkObject /\ _) :| rest) <-
          createArbitraryDAGWithDepth f (d - 1)
        link1 /\ e1 <- addIpfsObject ipfs refGenState [subtreeRootLinkObject]
        link2 /\ e2 <- addIpfsObject ipfs refGenState [subtreeRootLinkObject]
        newRoot@(link /\ e) <- addIpfsObject ipfs refGenState [link1, link2]

        -- account for the double counting of size
        let
          size' =
            unsafeFromInt $
              bigIntToInt link.size - bigIntToInt subtreeRootLinkObject.size
          link' =
            link {size = size'}
          newRoot' =
            link' /\ e
        pure $
          DAG (newRoot' :|
                (link1 /\ e1) : (link2 /\ e2) : subtreeHead : rest)

    Fork -> do
        -- extend with a new root forking to two subtrees of equal depth
        firstSubtreeFiber <- fork $ createArbitraryDAGWithDepth f (d - 1)

        let
          perturbSeed :: GenState -> GenState
          perturbSeed state =
            state {newSeed = mkSeed $ unSeed state.newSeed + 1}
        refGenState' <- liftEffect $ new $ perturbSeed genState'
        secondSubtreeFiber <- fork $
          withReaderT (_ {refGenState = refGenState'}) $ -- use alternate state
            createArbitraryDAGWithDepth f (d - 1)

        DAG (h1@(lo1 /\ e1) :| r1) <- join firstSubtreeFiber
        DAG (h2@(lo2 /\ e2) :| r2) <- join secondSubtreeFiber

        newRoot@(link /\ e) <- addIpfsObject ipfs refGenState [lo1, lo2]
        pure $
          DAG (newRoot :| union (h1 : r1) (h2 : r2))

concurrentlyAccessArbitraryObjects
  :: Int
  -> Array (Tuple LinkObject E)
  -> IpfsGenContext Int
concurrentlyAccessArbitraryObjects maxObjectsToAccess pairs = do
  {ipfs, refGenState} <- ask
  genState            <- liftEffect $ read refGenState

  let
    shuffledPairs /\ genState' =
      runGen (shuffle pairs) genState
    objs =
      A.take maxObjectsToAccess shuffledPairs
    nAccessed =
      A.length objs
  liftEffect $ write genState' refGenState

  parTraverse_ <@> objs $ \({hash} /\ (E e)) -> do
    stat <- liftAff $ runNoLoggingT $ expectRight =<< runExceptT do
      Ipfs.Api.Commands.Object.Stat.run <@> ipfs $ {hash}
    liftAff $ do
      stat.hash `shouldEqual` hash
      stat.numLinks `shouldEqual` (A.length e.links)

    o <- liftAff $ runNoLoggingT $ expectRight =<< runExceptT do
      Ipfs.Api.Commands.Object.Get.run <@> ipfs $ {hash}
    liftAff $ do
      o."data" `shouldEqual` e.d
      o.links `shouldEqual` e.links

  pure nAccessed

-- | emit DAG suitable for rendering by GraphViz dot. Save to file and
-- |   $ dot -Tpdf <filename.dot> -O
-- | output will reside under <filename.dot>.pdf
toDot :: DAG -> String
toDot (DAG (h:|r)) =
  "digraph {\n" <> foldr nodeTupleToParents "}\n" (h:r)
  where
    nodeTupleToParents :: Tuple LinkObject E -> String -> String
    nodeTupleToParents ({hash} /\ (E { links })) =
      edgesFromTo hash links
    edgesFromTo
      :: Ipfs.Base58Encoded Ipfs.Multihash
      -> Array LinkObject
      -> String
      -> String
    edgesFromTo node [] s =
      "  \"" <> (shorten <<< Ipfs.toString) node <> "\";\n" <> s
    edgesFromTo node ps s =
      foldr
        (\p str -> "  \"" <> (shorten <<< Ipfs.toString) node <> "\" -> \"" <>
          (shorten <<< Ipfs.toString) p.hash <> "\";\n" <> str
        )
        s
        ps
