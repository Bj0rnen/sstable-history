import Diagrams.Prelude
import Diagrams.Backend.Cairo.CmdLine
import Codec.Picture.Gif

rotatedSquare :: Double -> Diagram Cairo
rotatedSquare rot = rotate (rot @@ deg) unitSquare # fc blue

myAnim :: [(Diagram Cairo, GifDelay)]
myAnim = zip (map rotatedSquare [0..89]) (repeat 3)

main = gifMain myAnim
