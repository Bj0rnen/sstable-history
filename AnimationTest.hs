{-# LANGUAGE PartialTypeSignatures #-}
import Diagrams.Prelude
import Diagrams.Backend.Cairo.CmdLine

myAnim :: Animation B V2 Double
myAnim = mkActive 0 1 (\rot -> rotate ((fromRational $ fromTime rot) @@ turn) unitSquare)

main = animMain myAnim
