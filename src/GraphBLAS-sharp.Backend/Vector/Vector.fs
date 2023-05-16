namespace GraphBLAS.FSharp.Backend.Vector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
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
                          ) // TODO empty vector
                      Size = size }
            | Dense ->
                ClVector.Dense
                <| zeroCreate processor allocationMode size

    let ofList (clContext: ClContext) workGroupSize =
        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let map =
            ClArray.map <@ Some @> clContext workGroupSize

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

    let map2 (opAdd: Expr<'a option -> 'b option -> 'c option>) (clContext: ClContext) workGroupSize =
        let map2Dense =
            Dense.Vector.map2 opAdd clContext workGroupSize

        let map2Sparse =
            Sparse.Vector.map2 opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| map2Dense processor allocationMode left right
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| map2Sparse processor allocationMode left right
            | _ -> failwith "Vector formats are not matching."

    let map2AtLeastOne (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) (clContext: ClContext) workGroupSize =
        let map2Sparse =
            Sparse.Vector.map2AtLeastOne opAdd clContext workGroupSize

        let map2Dense =
            Dense.Vector.map2AtLeastOne opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| map2Sparse processor allocationMode left right
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| map2Dense processor allocationMode left right
            | _ -> failwith "Vector formats are not matching."

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

    let reduce (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
        let sparseReduce =
            Sparse.Vector.reduce opAdd clContext workGroupSize

        let denseReduce =
            Dense.Vector.reduce opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector -> sparseReduce processor vector
            | ClVector.Dense vector -> denseReduce processor vector
