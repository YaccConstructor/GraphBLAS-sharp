namespace GraphBLAS.FSharp

open Brahma.FSharp
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClVector
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector

[<RequireQualifiedAccess>]
module Vector =
    /// <summary>
    /// Builds vector of given format with fixed size and fills it with the default values of desired type.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let zeroCreate (clContext: ClContext) workGroupSize =
        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode size format ->
            match format with
            | Sparse ->
                ClVector.Sparse
                    { Context = clContext
                      Indices = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, [| 0 |])
                      Values =
                          clContext.CreateClArrayWithSpecificAllocationMode(
                              allocationMode,
                              [| Unchecked.defaultof<'a> |]
                          ) // TODO empty vector
                      Size = size }
            | Dense ->
                ClVector.Dense
                <| zeroCreate processor allocationMode size

    /// <summary>
    /// Builds vector of given format with fixed size and fills it with the values from the given list.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let ofList (clContext: ClContext) workGroupSize =
        let scatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let map =
            Map.map <@ Some @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode format size (elements: (int * 'a) list) ->
            match format with
            | Sparse ->
                let indices, values =
                    elements
                    |> Array.ofList
                    |> Array.sortBy fst
                    |> Array.unzip

                { Context = clContext
                  Indices = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, indices)
                  Values = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, values)
                  Size = size }
                |> ClVector.Sparse
            | Dense ->
                let indices, values = elements |> Array.ofList |> Array.unzip

                let values =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

                let indices =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, indices)

                let mappedValues = map processor DeviceOnly values

                let result = zeroCreate processor allocationMode size

                scatter processor indices mappedValues result

                processor.Post(Msg.CreateFreeMsg(mappedValues))
                processor.Post(Msg.CreateFreeMsg(indices))
                processor.Post(Msg.CreateFreeMsg(values))

                ClVector.Dense result

    /// <summary>
    /// Creates new vector with the values from the given one.
    /// New vector represented in the format of the given one.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let copy (clContext: ClContext) workGroupSize =
        let sparseCopy =
            Sparse.Vector.copy clContext workGroupSize

        let copyOptionData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector ->
                ClVector.Sparse
                <| sparseCopy processor allocationMode vector
            | ClVector.Dense vector ->
                ClVector.Dense
                <| copyOptionData processor allocationMode vector

    /// <summary>
    /// Sparsifies the given vector if it is in a dense format.
    /// If the given vector is already sparse, copies it.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toSparse (clContext: ClContext) workGroupSize =
        let toSparse =
            Dense.Vector.toSparse clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector ->
                ClVector.Sparse
                <| toSparse processor allocationMode vector
            | ClVector.Sparse _ -> copy processor allocationMode vector

    /// <summary>
    /// Densifies the given vector if it is in a sparse format.
    /// If the given vector is already dense, copies it.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toDense (clContext: ClContext) workGroupSize =
        let toDense =
            Sparse.Vector.toDense clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector ->
                ClVector.Dense
                <| copy processor allocationMode vector
            | ClVector.Sparse vector ->
                ClVector.Dense
                <| toDense processor allocationMode vector

    let private assignByMaskGeneral<'a, 'b when 'a: struct and 'b: struct> op (clContext: ClContext) workGroupSize =

        let sparseFillVector =
            Sparse.Vector.assignByMask op clContext workGroupSize

        let denseFillVector =
            Dense.Vector.assignByMask op clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) (mask: ClVector<'b>) (value: ClCell<'a>) ->
            match vector, mask with
            | ClVector.Sparse vector, ClVector.Sparse mask ->
                ClVector.Sparse
                <| sparseFillVector processor allocationMode vector mask value
            | ClVector.Dense vector, ClVector.Dense mask ->
                ClVector.Dense
                <| denseFillVector processor allocationMode vector mask value
            | _ -> failwith "Vector formats are not matching."

    let assignByMask<'a, 'b when 'a: struct and 'b: struct> op clContext workGroupSize =
        assignByMaskGeneral<'a, 'b> (Convert.assignToOption op) clContext workGroupSize

    let assignByMaskComplemented<'a, 'b when 'a: struct and 'b: struct> op clContext workGroupSize =
        assignByMaskGeneral<'a, 'b> (Convert.assignComplementedToOption op) clContext workGroupSize

    /// <summary>
    /// Applies a function to each value of the vector, threading an accumulator argument through the computation.
    /// Begin by applying the function to the first two values.
    /// Then feed this result into the function along with the third value and so on.
    /// Return the final result.
    /// </summary>
    /// <remarks>
    /// Implicit zeroes are ignored during the computation.
    /// </remarks>
    /// <param name="opAdd"></param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let reduce (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
        let sparseReduce =
            Sparse.Vector.reduce opAdd clContext workGroupSize

        let denseReduce =
            Dense.Vector.reduce opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector -> sparseReduce processor vector
            | ClVector.Dense vector -> denseReduce processor vector
