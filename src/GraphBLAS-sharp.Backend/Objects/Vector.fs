namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend

type VectorFormat =
    | COO
    | Dense

type COOVector<'a> =
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

        COOVector.FromTuples(indices, vals, array.Length)

and ClCooVector<'a> =
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

type DenseVector<'a> =
    { Values: 'a option [] }

    member this.Size = this.Values.Length

    override this.ToString() =
        [ sprintf "Dense Vector\n"
          sprintf "Size:    %i \n" this.Values.Length
          sprintf "Values:  %A \n" this.Values ]
        |> String.concat ""

    member this.ToDevice(context: ClContext) =
        { ClDenseVector.Values = context.CreateClArray this.Values }

    static member FromArray(array: 'a [], isZero: 'a -> bool) =
        { Values =
              array
              |> Array.map (fun v -> if isZero v then None else Some v) }

and ClDenseVector<'a> =
    { Values: ClArray<'a option> }

    member this.Size = this.Values.Length

    member this.ToHost(q: MailboxProcessor<_>) =
        let vector = Array.zeroCreate this.Values.Length

        let _ =
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(this.Values, vector, ch))

        { DenseVector.Values = vector }

    interface IDeviceMemObject with
        member this.Dispose(q) =
            q.Post(Msg.CreateFreeMsg<_>(this.Values))
            q.PostAndReply(Msg.MsgNotifyMe)

    member this.Dispose(q) = (this :> IDeviceMemObject).Dispose(q)

    static member FromArray(context: ClContext, array: 'a option []) =
        { Values = context.CreateClArray array }

type TuplesVector<'a> =
    { Indices: int []
      Values: 'a []
      Size: int }

    override this.ToString() =
        [ sprintf "Tuples Vector\n"
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

and ClTuplesVector<'a> =
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

type Vector<'a when 'a: struct> =
    | VectorCOO of COOVector<'a>
    | VectorDense of DenseVector<'a>
    member this.Size =
        match this with
        | VectorCOO vector -> vector.Size
        | VectorDense vector -> vector.Size

    member this.ToDevice(context: ClContext) =
        match this with
        | VectorCOO vector -> ClVectorCOO <| vector.ToDevice(context)
        | VectorDense vector -> ClVectorDense <| vector.ToDevice(context)

and ClVector<'a when 'a: struct> =
    | ClVectorCOO of ClCooVector<'a>
    | ClVectorDense of ClDenseVector<'a>
    member this.Size =
        match this with
        | ClVectorCOO vector -> vector.Size
        | ClVectorDense vector -> vector.Size

    member this.ToHost(q: MailboxProcessor<_>) =
        match this with
        | ClVectorCOO vector -> VectorCOO <| vector.ToHost(q)
        | ClVectorDense vector -> VectorDense <| vector.ToHost(q)

    member this.Dispose(q) =
        match this with
        | ClVectorCOO vector -> vector.Dispose(q)
        | ClVectorDense vector -> vector.Dispose(q)
