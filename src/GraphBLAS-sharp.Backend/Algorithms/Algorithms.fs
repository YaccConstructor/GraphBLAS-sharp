namespace GraphBLAS.FSharp

open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Backend.Algorithms

[<RequireQualifiedAccess>]
module Algorithms =
    let singleSourceBFS = BFS.singleSource
