namespace GraphBLAS.FSharp.Backend.Vector

open Brahma.FSharp
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Backend.Vector.Sparse
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Common.ClArray

module Vector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int) =
        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (size: int) (format: VectorFormat) ->
            match format with
            | Sparse ->
                let vector =
                    { Context = clContext
                      Indices = clContext.CreateClArrayWithGPUOnlyFlags [| 0 |]
                      Values = clContext.CreateClArrayWithGPUOnlyFlags [| Unchecked.defaultof<'a> |]
                      Size = size }

                ClVectorSparse vector
            | Dense -> ClVectorDense <| zeroCreate processor size

    let ofList (clContext: ClContext) =
        fun (format: VectorFormat) size (elements: (int * 'a) list) ->
            let indices, values =
                elements
                |> Array.ofList
                |> Array.sortBy fst
                |> Array.unzip

            match format with
            | Sparse ->
                SparseVector
                    .FromTuples(indices, values, size)
                    .ToDevice clContext
                |> ClVectorSparse
            | Dense ->
                let res = Array.zeroCreate size

                for i in 0 .. indices.Length - 1 do
                    res.[indices.[i]] <- Some(values.[i])

                ClVectorDense
                <| clContext.CreateClArrayWithGPUOnlyFlags res

    let copy (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        let copyOptionData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorSparse vector ->
                let vector =
                    { Context = clContext
                      Indices = copy processor vector.Indices
                      Values = copyData processor vector.Values
                      Size = vector.Size }

                ClVectorSparse vector
            | ClVectorDense vector -> ClVectorDense <| copyOptionData processor vector

    let mask = copy

    let toSparse (clContext: ClContext) (workGroupSize: int) =
        let toSparse =
            DenseVector.toSparse clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorDense vector -> ClVectorSparse <| toSparse processor vector
            | ClVectorSparse _ -> copy processor vector

    let toDense (clContext: ClContext) (workGroupSize: int) =
        let toDense =
            SparseVector.toDense clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorDense vector -> ClVectorDense <| copy processor vector
            | ClVectorSparse vector -> ClVectorDense <| toDense processor vector

    let elementWiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let addSparse =
            SparseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        let addDense =
            DenseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVectorSparse left, ClVectorSparse right -> ClVectorSparse <| addSparse processor left right
            | ClVectorDense left, ClVectorDense right -> ClVectorDense <| addDense processor left right
            | _ -> failwith "Vector formats are not matching."

    let elementWise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) (workGroupSize: int) =
        let addDense =
            DenseVector.elementWise clContext opAdd workGroupSize

        let addSparse =
            SparseVector.elementWise clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVectorDense leftVector, ClVectorDense rightVector ->
                ClVectorDense
                <| addDense processor leftVector rightVector
            | ClVectorSparse left, ClVectorSparse right -> ClVectorSparse <| addSparse processor left right
            | _ -> failwith "Vector formats are not matching."

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> maskOp (clContext: ClContext) (workGroupSize: int) =
        let sparseFillVector =
            SparseVector.fillSubVector clContext (StandardOperations.fillSubToOption maskOp) workGroupSize

        let denseFillVector =
            DenseVector.fillSubVector clContext (StandardOperations.fillSubToOption maskOp) workGroupSize

        let toSparseVector =
            DenseVector.toSparse clContext workGroupSize

        let toSparseMask =
            DenseVector.toSparse clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match vector, maskVector with
            | ClVectorSparse vector, ClVectorSparse mask ->
                ClVectorSparse
                <| sparseFillVector processor vector mask value
            | ClVectorSparse vector, ClVectorDense mask ->
                let mask = toSparseMask processor mask

                ClVectorSparse
                <| sparseFillVector processor vector mask value
            | ClVectorDense vector, ClVectorSparse mask ->
                let vector = toSparseVector processor vector

                ClVectorSparse
                <| sparseFillVector processor vector mask value
            | ClVectorDense vector, ClVectorDense mask ->
                ClVectorDense
                <| denseFillVector processor vector mask value

    let fillSubVectorComplemented<'a, 'b when 'a: struct and 'b: struct>
        maskOp
        (clContext: ClContext)
        (workGroupSize: int)
        =

        let denseFillVector =
            DenseVector.fillSubVector clContext (StandardOperations.fillSubComplementedToOption maskOp) workGroupSize

        let vectorToDense =
            SparseVector.toDense clContext workGroupSize

        let maskToDense =
            SparseVector.toDense clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match leftVector, maskVector with
            | ClVectorSparse vector, ClVectorSparse mask ->
                let denseVector = vectorToDense processor vector
                let denseMask = maskToDense processor mask

                ClVectorDense
                <| denseFillVector processor denseVector denseMask value
            | ClVectorDense vector, ClVectorSparse mask ->
                let denseMask = maskToDense processor mask

                ClVectorDense
                <| denseFillVector processor vector denseMask value
            | ClVectorSparse vector, ClVectorDense mask ->
                let denseVector = vectorToDense processor vector

                ClVectorDense
                <| denseFillVector processor denseVector mask value
            | ClVectorDense vector, ClVectorDense mask ->
                ClVectorDense
                <| denseFillVector processor vector mask value

    let standardFillSubVector<'a, 'b when 'a: struct and 'b: struct> =
        fillSubVector<'a, 'b> StandardOperations.fillSubOp<'a>

    let standardFillSubVectorComplemented<'a, 'b when 'a: struct and 'b: struct> =
        fillSubVectorComplemented<'a, 'b> StandardOperations.fillSubOp<'a>

    let reduce (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =
        let sparseReduce =
            SparseVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorSparse vector -> sparseReduce processor vector
            | ClVectorDense vector -> denseReduce processor vector
