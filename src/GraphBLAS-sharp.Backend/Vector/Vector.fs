namespace GraphBLAS.FSharp.Backend.Vector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Backend.Vector.Sparse
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClVector


module Vector =
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
                          )
                      Size = size }
            | Dense ->
                ClVector.Dense
                <| zeroCreate processor allocationMode size

    let ofList (clContext: ClContext) workGroupSize =
        let scatter =
            Scatter.runInplace clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let map =
            ClArray.map clContext workGroupSize <@ Some @>

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

    let copy (clContext: ClContext) workGroupSize =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        let copyOptionData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector ->
                { Context = clContext
                  Indices = copy processor allocationMode vector.Indices
                  Values = copyData processor allocationMode vector.Values
                  Size = vector.Size }
                |> ClVector.Sparse
            | ClVector.Dense vector ->
                ClVector.Dense
                <| copyOptionData processor allocationMode vector

    let mask = copy

    let toSparse (clContext: ClContext) workGroupSize =
        let toSparse =
            DenseVector.toSparse clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector ->
                ClVector.Sparse
                <| toSparse processor allocationMode vector
            | ClVector.Sparse _ -> copy processor allocationMode vector

    let toDense (clContext: ClContext) workGroupSize =
        let toDense =
            SparseVector.toDense clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector ->
                ClVector.Dense
                <| copy processor allocationMode vector
            | ClVector.Sparse vector ->
                ClVector.Dense
                <| toDense processor allocationMode vector

    let map2 (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let addDense =
            DenseVector.map2 clContext opAdd workGroupSize

        let addSparse =
            SparseVector.map2 clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| addDense processor allocationMode left right
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| addSparse processor allocationMode left right
            | _ -> failwith "Vector formats are not matching."

    let map2AtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let addSparse =
            SparseVector.map2AtLeastOne clContext opAdd workGroupSize

        let addDense =
            DenseVector.map2AtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| addSparse processor allocationMode left right
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| addDense processor allocationMode left right
            | _ -> failwith "Vector formats are not matching."

    let map2General<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupsSize
        =

        let sparseEWise =
            SparseVector.map2General clContext opAdd workGroupsSize

        let denseEWise =
            DenseVector.map2 clContext opAdd workGroupsSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| sparseEWise processor allocationMode left right
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| denseEWise processor allocationMode left right
            | _ -> failwith "Vector formats are not matching."

    let private assignByMaskGeneral<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) op workGroupSize =

        let sparseFillVector =
            SparseVector.assignByMask clContext op workGroupSize

        let denseFillVector =
            DenseVector.assignByMask clContext op workGroupSize

        let toSparseVector =
            DenseVector.toSparse clContext workGroupSize

        let toSparseMask =
            DenseVector.toSparse clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector<'a>) (mask: ClVector<'b>) (value: ClCell<'a>) ->
            match vector, mask with
            | ClVector.Sparse vector, ClVector.Sparse mask ->
                ClVector.Sparse
                <| sparseFillVector processor allocationMode vector mask value
            | ClVector.Sparse vector, ClVector.Dense mask ->
                let mask =
                    toSparseMask processor allocationMode mask

                ClVector.Sparse
                <| sparseFillVector processor allocationMode vector mask value
            | ClVector.Dense vector, ClVector.Sparse mask ->
                let vector =
                    toSparseVector processor allocationMode vector

                ClVector.Sparse
                <| sparseFillVector processor allocationMode vector mask value
            | ClVector.Dense vector, ClVector.Dense mask ->
                ClVector.Dense
                <| denseFillVector processor allocationMode vector mask value

    let assignByMask<'a, 'b when 'a: struct and 'b: struct> clContext op workGroupSize =
        assignByMaskGeneral<'a, 'b> clContext (Convert.assignToOption op) workGroupSize

    let assignByMaskComplemented<'a, 'b when 'a: struct and 'b: struct> clContext op workGroupSize =
        assignByMaskGeneral<'a, 'b> clContext (Convert.assignComplementedToOption op) workGroupSize

    let reduce (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =
        let sparseReduce =
            SparseVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector -> sparseReduce processor vector
            | ClVector.Dense vector -> denseReduce processor vector
