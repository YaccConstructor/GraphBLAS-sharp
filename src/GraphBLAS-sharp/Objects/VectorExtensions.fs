namespace GraphBLAS.FSharp.Objects

open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.Vector
open GraphBLAS.FSharp.Objects.ArraysExtensions

module ClVectorExtensions =
    type ClVector.Sparse<'a> with
        member this.ToHost(q: MailboxProcessor<_>) =
            { Indices = this.Indices.ToHost q
              Values = this.Values.ToHost q
              Size = this.Size }

    type ClVector<'a when 'a: struct> with
        member this.ToHost(q: MailboxProcessor<_>) =
            match this with
            | ClVector.Sparse vector -> Vector.Sparse <| vector.ToHost q
            | ClVector.Dense vector -> Vector.Dense <| vector.ToHost q
