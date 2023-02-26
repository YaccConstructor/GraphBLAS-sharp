namespace GraphBLAS.FSharp.Objects

open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects
open Brahma.FSharp

module ClVectorExtensions =
    type ClVector<'a when 'a: struct> with
        member this.ToHost(q: MailboxProcessor<_>) =
            match this with
            | ClVector.Sparse vector ->
                let indices = Array.zeroCreate vector.Indices.Length
                let values = Array.zeroCreate vector.Values.Length

                q.Post(Msg.CreateToHostMsg(vector.Indices, indices))

                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(vector.Values, values, ch))
                |> ignore

                Vector.Sparse
                <| { Indices = indices
                     Values = values
                     Size = this.Size }
            | ClVector.Dense vector -> Vector.Dense <| vector.ToHost q
