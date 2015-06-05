{-# LANGUAGE DataKinds, OverloadedStrings, GADTs #-}
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Int
import Data.Text (Text)
import Data.Time.Clock
import Data.Set (Set)
import qualified Data.Set as Set
import Control.Arrow
import Control.Applicative
import Control.Monad.IO.Class
import Data.List

import Database.Cassandra.CQL

import Diagrams.Prelude hiding (Query, size)
import Diagrams.Backend.Cairo.CmdLine

--(&) = flip ($)

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



data Existence = Flushed | Compacted | Idle | Replaced
    deriving (Show, Eq)

stabilize Flushed = Idle
stabilize Compacted = Idle
stabilize x = x

settle m =
    m
    & Map.filter ((/= Replaced) . snd)
    & Map.map (second stabilize)

progression :: [SSTableMetadata] -> [Map Generation (SSTableMetadata, Existence)]
progression = scanl (
    \m md ->
        let settled = settle m in -- Might want the Map.update function. It can remove elements!
        case (generation md, ancestors md) of
            (gen, Nothing) -> Map.insert gen (md, Flushed) settled
            (gen, Just replaced) ->
                Set.foldr (Map.adjust (\(x, _) -> (x, Replaced))) settled replaced
                & Map.insert gen (md, Compacted)
    ) Map.empty



















drawRectangle x y w h =
    rect w h # translate (r2 (x + w / 2, y - h / 2))

-- middle element or right of middle, if even length
middle [] = Nothing
middle xs = Just $ xs !! (length xs `div` 2)

median xs = middle $ sort xs

timespan sstable =
    fromIntegral $ maxTimestamp' - minTimestamp' + 1
    where
        minTimestamp' = minTimestamp sstable
        maxTimestamp' = maxTimestamp sstable

sstableHeight sstable =
    if timespan' == 0 then
        0
    else
        size' / timespan'
    where
        size' = fromIntegral $ size sstable
        timespan' = timespan sstable

drawSSTable globalMinTimestamp height sstable =
    drawRectangle
        timeOffset
        0
        (timespan sstable)
        height
    where
        timeOffset = fromIntegral $ minTimestamp sstable - globalMinTimestamp


drawColoredSSTable globalMinTimestamp height (sstable, existence) =
    drawSSTable globalMinTimestamp height sstable # fc color # lc color
    where
        color = case existence of
            Flushed -> blue
            Compacted -> green
            Replaced -> red
            Idle -> white



drawByTimestamps :: [(SSTableMetadata, Existence)] -> Diagram Cairo
drawByTimestamps sstables =
    let globalMinTimestamp = minimum $ map (minTimestamp . fst) sstables
        --globalMaxTimestamp = maximum $ map (maxTimestamp . fst) sstables
        magicHeight = 2715794001
        magicSeparation = 2715794001
        in
    vcat' (with & sep .~ magicSeparation) $ map (drawColoredSSTable globalMinTimestamp magicHeight) sstables




main = do
    pool <- newPool [("127.0.0.1", "9042")] "system" Nothing
    runCas pool $ do
        sstables <- map tupleToSSTableMetadata <$> executeRows QUORUM selectSSTableMetadatas ("system", "sstable_metadata")
        liftIO $ do
            --mapM_ print $ progression sstables
            let frames = map (drawByTimestamps . Map.elems) $ progression sstables
                globalMinTimestamp = minimum $ map minTimestamp sstables
                globalMaxTimestamp = maximum $ map maxTimestamp sstables
                bottomRight = drawRectangle (fromIntegral (globalMaxTimestamp - globalMinTimestamp)) (-(fromIntegral $ length sstables * 2715794001 * 2)) 0 0 :: Diagram Cairo
                frames' = map (bottomRight <>) frames
                -- That was an ugly hack
            gifMain $ zip frames' (repeat 100)
