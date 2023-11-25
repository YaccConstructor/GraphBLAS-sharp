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

/// <summary>
/// Represents an abstraction over vector, whose values and indices are in OpenCL device memory.
/// </summary>
[<RequireQualifiedAccess>]
type ClVector<'a when 'a: struct> =
    /// <summary>
    /// Represents an abstraction over sparse vector, whose values and indices are in OpenCL device memory.
    /// </summary>
    | Sparse of ClVector.Sparse<'a>
    /// <summary>
    /// Represents an abstraction over dense vector, whose values and indices are in OpenCL device memory.
    /// </summary>
    | Dense of ClArray<'a option>

    /// <summary>
    /// Gets the number of elements in vector.
    /// </summary>
    member this.Size =
        match this with
        | Sparse vector -> vector.Size
        | Dense vector -> vector.Size

    /// <summary>
    /// Release device resources allocated for the vector.
    /// </summary>
    member this.Dispose(q) =
        match this with
        | Sparse vector -> vector.Dispose(q)
        | Dense vector -> vector.FreeAndWait(q)
