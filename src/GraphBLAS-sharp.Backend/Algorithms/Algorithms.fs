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

    module PageRank =
        /// <summary>
        /// Computes PageRank of the given matrix.
        /// Matrix should be prepared in advance using "PageRank.prepareMatrix" method.
        /// Accepts accuracy as a parameter which determines how many iterations will be performed.
        /// Values of accuracy lower than 1e-06 are not recommended since the process may never stop.
        /// </summary>
        /// <example>
        /// <code>
        /// let preparedMatrix = PageRank.prepareMatrix clContext workGroupSize queue matrix
        /// let accuracy = 1e-05
        /// let pageRank = PageRank.run clContext workGroupSize queue preparedMatrix accuracy
        /// </code>
        /// </example>
        let run = PageRank.run

        /// <summary>
        /// Converts matrix representing a graph to a format suitable for PageRank algorithm.
        /// </summary>
        let prepareMatrix = PageRank.prepareMatrix
