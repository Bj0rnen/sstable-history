{-# LANGUAGE DataKinds, OverloadedStrings #-}
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Int
import Data.Text (Text)
import Data.Time.Clock
import Data.Set (Set)
import qualified Data.Set as Set
import Control.Arrow
import Control.Applicative
import Control.Monad.IO.Class

import Database.Cassandra.CQL

(&) = flip ($)

type Generation = Int
--type Metadata = (Generation, [Generation])

type KeyspaceName = Text
type TableName = Text
data SSTableMetadata = SSTableMetadata
    { keyspaceName :: KeyspaceName
    , tableName    :: TableName
    , generation   :: Generation
    , ancestors    :: Maybe (Set Generation)
    , arrival      :: UTCTime
    , level        :: Int
    , minTimestamp :: Int64
    , maxTimestamp :: Int64
    , origin       :: Text
    , size         :: Int64
    }
    deriving (Show)
type SSTableMetadataTuple = (KeyspaceName, TableName, Int, Maybe (Set Int), UTCTime, Int, Int64, Int64, Text, Int64)


tupleToSSTableMetadata (keyspaceName, tableName, generation, ancestores, arrival, level, minTimestamp, maxTimestamp, origin, size) =
    SSTableMetadata keyspaceName tableName generation ancestores arrival level minTimestamp maxTimestamp origin size

selectSSTableMetadatas :: Query Rows (KeyspaceName, TableName) SSTableMetadataTuple
selectSSTableMetadatas =
    "SELECT \
    \    keyspace_name, \
    \    table_name, \
    \    generation, \
    \    ancestors, \
    \    arrival, \
    \    level, \
    \    min_timestamp, \
    \    max_timestamp, \
    \    origin, \
    \    size \
    \FROM sstable_metadata \
    \WHERE keyspace_name = ? AND table_name = ?"



data Existence = Dumped | Compacted | Idle | Replaced
    deriving (Show, Eq)

stabilize Dumped = Idle
stabilize Compacted = Idle
stabilize x = x

settle m =
    m
    & Map.filter ((/= Replaced) . snd)
    & Map.map (second stabilize)

progression :: [SSTableMetadata] -> [Map Generation (SSTableMetadata, Existence)]
progression = scanl (
    \m md ->
        let settled = settle m in
        case (generation md, ancestors md) of
            (gen, Nothing) -> Map.insert gen (md, Dumped) settled
            (gen, Just replaced) ->
                Set.foldr (`Map.insert` (md, Replaced)) settled replaced
                & Map.insert gen (md, Compacted)
    ) Map.empty


main = do
    pool <- newPool [("127.0.0.1", "9042")] "system" Nothing
    runCas pool $ do
        sstables <- map tupleToSSTableMetadata <$> executeRows QUORUM selectSSTableMetadatas ("system", "sstable_metadata")
        liftIO $ mapM_ print $ progression sstables
