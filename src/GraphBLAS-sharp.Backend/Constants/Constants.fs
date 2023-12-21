namespace GraphBLAS.FSharp

[<RequireQualifiedAccess>]
module Constants =
    module PageRank =
        /// <summary>
        /// PageRank algorithms will finish then
        /// difference of current and previous vectors
        /// is less than accuracy
        /// </summary>
        let accuracy = 1e-6f
        /// <summary>
        /// Damping factor for PageRank algorithm
        /// </summary>
        let alpha = 0.85f

    module Common =
        let defaultWorkGroupSize = 32
