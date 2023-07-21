namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Objects.ArraysExtensions

type VectorFormat =
    | Sparse
    | Dense

module ClVector =
    type Sparse<'a> =
        { Context: ClContext
          Indices: ClArray<int>
          Values: ClArray<'a>
          Size: int }

        interface IDeviceMemObject with
            member this.Dispose(q) =
                q.Post(Msg.CreateFreeMsg<_>(this.Values))
                q.Post(Msg.CreateFreeMsg<_>(this.Indices))
                q.PostAndReply(Msg.MsgNotifyMe)

        member this.Dispose(q) = (this :> IDeviceMemObject).Dispose(q)

        member this.NNZ = this.Values.Length

[<RequireQualifiedAccess>]
type ClVector<'a when 'a: struct> =
    | Sparse of ClVector.Sparse<'a>
    | Dense of ClArray<'a option>
    member this.Size =
        match this with
        | Sparse vector -> vector.Size
        | Dense vector -> vector.Size

    member this.Dispose(q) =
        match this with
        | Sparse vector -> vector.Dispose(q)
        | Dense vector -> vector.FreeAndWait(q)
