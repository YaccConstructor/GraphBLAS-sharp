namespace GraphBLAS.FSharp.Backend.Algorithms

open Brahma.FSharp
open GraphBLAS.FSharp.Objects

module PageRank =
    [<Sealed>]
    type PageRankMatrix =
        member Dispose : MailboxProcessor<Msg> -> unit

    val internal prepareMatrix : ClContext -> int -> (MailboxProcessor<Msg> -> ClMatrix<float32> -> PageRankMatrix)

    val internal run : ClContext -> int -> (MailboxProcessor<Msg> -> PageRankMatrix -> float32 -> ClVector<float32>)
