module LogMetadata
  ( createArbitraryLogs
  , LogMetadata(..)
  , Log
  ) where

import Prelude

import Common (IpfsGenContext)
import Control.Parallel (parTraverse)
import Dag (DAG(..), DAGExtensionFrequencies, createArbitraryDAGWithDepth)
import Data.Array (range)
import Data.NonEmpty ((:|))
import Data.Tuple.Nested ((/\))
import Data.UUID (UUID, genUUID)
import Effect.Class (liftEffect)
import Ipfs.Api.Commands.Object.Internal (LinkObject)

type Log
  = { logId :: UUID
    , link  :: LinkObject
    }

type LogMetadata =
  { logs :: Array Log
  }

createArbitraryLogs
  :: DAGExtensionFrequencies
  -> Int
  -> Int
  -> IpfsGenContext LogMetadata
createArbitraryLogs dagShape numDags depth = do
  logs <- parTraverse <@> (range 1 numDags) $ \_ -> ado
      logId               <- liftEffect genUUID
      DAG ((h /\ _) :| _) <- createArbitraryDAGWithDepth dagShape depth
    in {logId, link: h}

  pure {logs}
