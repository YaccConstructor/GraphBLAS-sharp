namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClVector

module Vector =
    type Sparse<'a> =
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

            Sparse.FromTuples(indices, vals, array.Length)

        member this.NNZ = this.Values.Length

open Vector

[<RequireQualifiedAccess>]
type Vector<'a when 'a: struct> =
    | Sparse of Vector.Sparse<'a>
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

    member this.NNZ =
        match this with
        | Sparse vector -> vector.NNZ
        | Dense vector -> (Array.filter Option.isSome vector).Length
