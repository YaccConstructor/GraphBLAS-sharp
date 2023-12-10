namespace GraphBLAS.FSharp

open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Backend.Algorithms

[<RequireQualifiedAccess>]
module Algorithms =
    module BFS =
        let singleSource = BFS.singleSource

        let singleSourceSparse = BFS.singleSourceSparse

        let singleSourcePushPull = BFS.singleSourcePushPull

    module MSBFS =
        let runLevels = MSBFS.Levels.run

        let runParents = MSBFS.Parents.run

    module SSSP =
        let run = SSSP.run
