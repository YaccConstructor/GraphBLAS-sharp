namespace GraphBLAS.FSharp.Backend.Vector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Backend.Vector.Sparse
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Common.ClArray
open GraphBLAS.FSharp.Backend.Objects.ClVector


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

                ClVector.Sparse vector
            | Dense -> ClVector.Dense <| zeroCreate processor size

    let ofList (clContext: ClContext) =
        fun (format: VectorFormat) size (elements: (int * 'a) list) ->
            let indices, values =
                elements
                |> Array.ofList
                |> Array.sortBy fst
                |> Array.unzip

            match format with
            | Sparse ->
                let indices = clContext.CreateClArray indices
                let values = clContext.CreateClArray values

                { Context = clContext
                  Indices = indices
                  Values = values
                  Size = size }

                |> ClVector.Sparse
            | Dense ->
                let res = Array.zeroCreate size

                for i in 0 .. indices.Length - 1 do
                    res.[indices.[i]] <- Some(values.[i])

                ClVector.Dense
                <| clContext.CreateClArrayWithGPUOnlyFlags res

    let copy (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        let copyOptionData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector ->
                let vector =
                    { Context = clContext
                      Indices = copy processor vector.Indices
                      Values = copyData processor vector.Values
                      Size = vector.Size }

                ClVector.Sparse vector
            | ClVector.Dense vector -> ClVector.Dense <| copyOptionData processor vector

    let mask = copy

    let toSparse (clContext: ClContext) (workGroupSize: int) =
        let toSparse =
            DenseVector.toSparse clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector -> ClVector.Sparse <| toSparse processor vector
            | ClVector.Sparse _ -> copy processor vector

    let toDense (clContext: ClContext) (workGroupSize: int) =
        let toDense =
            SparseVector.toDense clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector -> ClVector.Dense <| copy processor vector
            | ClVector.Sparse vector -> ClVector.Dense <| toDense processor vector

    let elementWiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let addSparse =
            SparseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        let addDense =
            DenseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right -> ClVector.Sparse <| addSparse processor left right
            | ClVector.Dense left, ClVector.Dense right -> ClVector.Dense <| addDense processor left right
            | _ -> failwith "Vector formats are not matching."

    let elementWise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) (workGroupSize: int) =
        let addDense =
            DenseVector.elementWise clContext opAdd workGroupSize

        let addSparse =
            SparseVector.elementWise clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Dense left, ClVector.Dense right -> ClVector.Dense <| addDense processor left right
            | ClVector.Sparse left, ClVector.Sparse right -> ClVector.Sparse <| addSparse processor left right
            | _ -> failwith "Vector formats are not matching."

    let elementwiseGeneral<'a , 'b, 'c when 'a : struct and 'b : struct and 'c : struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupsSize =

        let sparseEWise =
            SparseVector.elementwiseGeneral clContext opAdd workGroupsSize

        let denseEWise =
            DenseVector.elementWise clContext opAdd workGroupsSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right -> ClVector.Sparse <| sparseEWise processor left right
            | ClVector.Dense left, ClVector.Dense right -> ClVector.Dense <| denseEWise processor left right
            | _ -> failwith "Vector formats are not matching."

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> maskOp (clContext: ClContext) (workGroupSize: int) =
        let sparseFillVector =
            SparseVector.fillSubVector clContext (Convert.fillSubToOption maskOp) workGroupSize

        let denseFillVector =
            DenseVector.fillSubVector clContext (Convert.fillSubToOption maskOp) workGroupSize

        let toSparseVector =
            DenseVector.toSparse clContext workGroupSize

        let toSparseMask =
            DenseVector.toSparse clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match vector, maskVector with
            | ClVector.Sparse vector, ClVector.Sparse mask ->
                ClVector.Sparse
                <| sparseFillVector processor vector mask value
            | ClVector.Sparse vector, ClVector.Dense mask ->
                let mask = toSparseMask processor mask

                ClVector.Sparse
                <| sparseFillVector processor vector mask value
            | ClVector.Dense vector, ClVector.Sparse mask ->
                let vector = toSparseVector processor vector

                ClVector.Sparse
                <| sparseFillVector processor vector mask value
            | ClVector.Dense vector, ClVector.Dense mask ->
                ClVector.Dense
                <| denseFillVector processor vector mask value

    let fillSubVectorComplemented<'a, 'b when 'a: struct and 'b: struct>
        maskOp
        (clContext: ClContext)
        (workGroupSize: int)
        =

        let denseFillVector =
            DenseVector.fillSubVector clContext (Convert.fillSubComplementedToOption maskOp) workGroupSize

        let vectorToDense =
            SparseVector.toDense clContext workGroupSize

        let maskToDense =
            SparseVector.toDense clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match leftVector, maskVector with
            | ClVector.Sparse vector, ClVector.Sparse mask ->
                let denseVector = vectorToDense processor vector
                let denseMask = maskToDense processor mask

                ClVector.Dense
                <| denseFillVector processor denseVector denseMask value
            | ClVector.Dense vector, ClVector.Sparse mask ->
                let denseMask = maskToDense processor mask

                ClVector.Dense
                <| denseFillVector processor vector denseMask value
            | ClVector.Sparse vector, ClVector.Dense mask ->
                let denseVector = vectorToDense processor vector

                ClVector.Dense
                <| denseFillVector processor denseVector mask value
            | ClVector.Dense vector, ClVector.Dense mask ->
                ClVector.Dense
                <| denseFillVector processor vector mask value

    let standardFillSubVector<'a, 'b when 'a: struct and 'b: struct> = fillSubVector<'a, 'b> Mask.fillSubOp<'a>

    let standardFillSubVectorComplemented<'a, 'b when 'a: struct and 'b: struct> =
        fillSubVectorComplemented<'a, 'b> Mask.fillSubOp<'a>

    let reduce (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =
        let sparseReduce =
            SparseVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector -> sparseReduce processor vector
            | ClVector.Dense vector -> denseReduce processor vector
