namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

module Vector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int) =
        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (size: int) (format: VectorFormat) ->
            match format with
            | Sparse ->
                let vector =
                    { Context = clContext
                      Indices = clContext.CreateClArray [| 0 |]
                      Values = clContext.CreateClArray [| Unchecked.defaultof<'a> |]
                      Size = size }

                ClVectorSparse vector
            | Dense -> ClVectorDense <| zeroCreate processor size

    let ofList (clContext: ClContext) =
        fun (format: VectorFormat) (elements: (int * 'a) list) ->
            let indices, values =
                elements
                |> Array.ofList
                |> Array.sortBy fst
                |> Array.unzip

            let resultLenght = (Array.max indices) + 1

            match format with
            | Sparse ->
                SparseVector
                    .FromTuples(indices, values, resultLenght)
                    .ToDevice clContext
                |> ClVectorSparse
            | Dense ->
                let res = Array.zeroCreate resultLenght

                for i in 0 .. indices.Length - 1 do
                    res.[indices.[i]] <- Some(values.[i])

                ClVectorDense <| clContext.CreateClArray res

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
            | ClVectorDense vector ->
                ClVectorDense <| copy processor vector
            | ClVectorSparse vector ->
                ClVectorDense <| toDense processor vector

    let elementWiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let addCoo =
            SparseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        let addDense =
            DenseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVectorSparse left, ClVectorSparse right -> ClVectorSparse <| addCoo processor left right
            | ClVectorDense left, ClVectorDense right -> ClVectorDense <| addDense processor left right
            | _ -> failwith "Vector formats are not matching."

    let elementWise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) (workGroupSize: int) =
        let addDense = DenseVector.elementWise clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVectorDense leftVector, ClVectorDense rightVector -> addDense processor leftVector rightVector
            | _ -> failwith "Vector formats are not matching."

    let fillSubVector (clContext: ClContext) (workGroupSize: int) =
        let cooFillVector =
            SparseVector.fillSubVector clContext workGroupSize

        let denseFillVector =
            DenseVector.fillSubVector clContext StandardOperations.mask workGroupSize

        let toCooVector =
            DenseVector.toSparse clContext workGroupSize

        let toCooMask =
            DenseVector.toSparse clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match vector, maskVector with
            | ClVectorSparse vector, ClVectorSparse mask ->
                ClVectorSparse
                <| cooFillVector value processor vector mask
            | ClVectorSparse vector, ClVectorDense mask ->
                let mask = toCooMask processor mask

                ClVectorSparse
                <| cooFillVector value processor vector mask
            | ClVectorDense vector, ClVectorSparse mask ->
                let vector = toCooVector processor vector

                ClVectorSparse
                <| cooFillVector value processor vector mask
            | ClVectorDense vector, ClVectorDense mask ->
                ClVectorDense
                <| denseFillVector processor vector mask value

    let reduce (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =
        let cooReduce =
            SparseVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorSparse vector -> cooReduce processor vector
            | ClVectorDense vector -> denseReduce processor vector
