namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

type VectorFormat =
    | Sparse
    | Dense

type SparseVector<'a> =
    { Indices: int []
      Values: 'a []
      Size: int }

    override this.ToString() =
        [ sprintf "Sparse Vector\n"
          sprintf "Size:    %i \n" this.Size
          sprintf "Indices: %A \n" this.Indices
          sprintf "Values:  %A \n" this.Values ]
        |> String.concat ""

    member this.ToDevice(context: ClContext) =
        let indices = context.CreateClArray this.Indices
        let values = context.CreateClArray this.Values

        { Context = context
          Indices = indices
          Values = values
          Size = this.Size }

    static member FromTuples(indices: int [], values: 'a [], size: int) =
        { Indices = indices
          Values = values
          Size = size }

    static member FromArray(array: 'a [], isZero: 'a -> bool) =
        let (indices, vals) =
            array
            |> Seq.cast<'a>
            |> Seq.mapi (fun idx v -> (idx, v))
            |> Seq.filter (fun (_, v) -> not (isZero v))
            |> Array.ofSeq
            |> Array.unzip

        SparseVector.FromTuples(indices, vals, array.Length)

and ClSparseVector<'a> =
    { Context: ClContext
      Indices: ClArray<int>
      Values: ClArray<'a>
      Size: int }

    member this.ToHost(q: MailboxProcessor<_>) =
        let indices = Array.zeroCreate this.Indices.Length
        let values = Array.zeroCreate this.Values.Length

        let _ =
            q.Post(Msg.CreateToHostMsg(this.Indices, indices))

        let _ =
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(this.Values, values, ch))

        { Indices = indices
          Values = values
          Size = this.Size }

    interface IDeviceMemObject with
        member this.Dispose(q) =
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.Post(Msg.CreateFreeMsg<_>(this.Indices))
            q.PostAndReply(Msg.MsgNotifyMe)

    member this.Dispose(q) = (this :> IDeviceMemObject).Dispose(q)

[<RequireQualifiedAccess>]
type Vector<'a when 'a: struct> =
    | Sparse of SparseVector<'a>
    | Dense of 'a option []
    member this.Size =
        match this with
        | Sparse vector -> vector.Size
        | Dense vector -> vector.Size

    override this.ToString() =
        match this with
        | Sparse vector -> vector.ToString()
        | Dense vector -> DenseVectorToString vector

    member this.ToDevice(context: ClContext) =
        match this with
        | Sparse vector -> ClVector.Sparse <| vector.ToDevice(context)
        | Dense vector -> ClVector.Dense <| vector.ToDevice(context)

and [<RequireQualifiedAccess>] ClVector<'a when 'a: struct> =
    | Sparse of ClSparseVector<'a>
    | Dense of ClArray<'a option>
    member this.Size =
        match this with
        | Sparse vector -> vector.Size
        | Dense vector -> vector.Size

    member this.ToHost(q: MailboxProcessor<_>) =
        match this with
        | Sparse vector -> Vector.Sparse <| vector.ToHost(q)
        | Dense vector -> Vector.Dense <| vector.ToHost(q)

    member this.Dispose(q) =
        match this with
        | Sparse vector -> vector.Dispose(q)
        | Dense vector -> vector.Dispose(q)
