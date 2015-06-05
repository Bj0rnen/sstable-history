{-# LANGUAGE LambdaCase #-}
import Data.Map as Map hiding (foldr)
import Control.Monad.State

type Gen = Integer
type Metadata = (Gen, [Gen])

data Existence = Dumped | Compacted | Idle | Replaced
    deriving (Show, Eq)

settle m =
    Map.map (\case Dumped -> Idle; Compacted -> Idle; Idle -> Idle; Replaced -> Replaced)$ Map.filter (/= Replaced) $ m

progression :: [Metadata] -> [Map Gen Existence]
progression = scanl (
    \m md ->
        let settled = settle m in
        case md of
            (gen, []) -> insert gen Dumped settled
            (gen, replaced) ->
                insert gen Compacted $ foldr (`insert` Replaced) settled replaced
    ) empty
