namespace GraphBLAS.FSharp.Objects

open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects

module ClVectorExtensions =
    type ClVector<'a when 'a: struct> with
        member this.ToHost(q: MailboxProcessor<_>) =
            match this with
            | ClVector.Sparse vector ->
                Vector.Sparse
                <| { Indices = vector.Indices.ToHost q
                     Values = vector.Values.ToHost q
                     Size = this.Size }
            | ClVector.Dense vector -> Vector.Dense <| vector.ToHost q

        member this.ToHostAndFree(q: MailboxProcessor<_>) =
           let result = this.ToHost q
           this.Dispose q

           result


