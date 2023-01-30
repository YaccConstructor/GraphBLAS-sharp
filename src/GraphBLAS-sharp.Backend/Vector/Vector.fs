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

        fun (processor: MailboxProcessor<_>) flag size format ->
            match format with
            | Sparse ->
                ClVector.Sparse
                    { Context = clContext
                      Indices = clContext.CreateClArrayWithFlag(flag, [| 0 |])
                      Values = clContext.CreateClArrayWithFlag(flag, [| Unchecked.defaultof<'a> |])
                      Size = size }
            | Dense -> ClVector.Dense <| zeroCreate processor flag size

    let ofList (clContext: ClContext) workGroupSize =
        let scatter =
            Scatter.runInplace clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let map =
            ClArray.map clContext workGroupSize <@ Some @>

        fun (processor: MailboxProcessor<_>) flag format size (elements: (int * 'a) list) ->
            match format with
            | Sparse ->
                let indices, values =
                    elements
                    |> Array.ofList
                    |> Array.sortBy fst
                    |> Array.unzip

                { Context = clContext
                  Indices = clContext.CreateClArrayWithFlag(flag, indices)
                  Values = clContext.CreateClArrayWithFlag(flag, values)
                  Size = size }
                |> ClVector.Sparse
            | Dense ->
                let indices, values = elements |> Array.ofList |> Array.unzip

                let values =
                    clContext.CreateClArrayWithFlag(DeviceOnly, values)

                let indices =
                    clContext.CreateClArrayWithFlag(DeviceOnly, indices)

                let mappedValues = map processor DeviceOnly values

                let result = zeroCreate processor flag size

                scatter processor indices mappedValues result

                processor.Post(Msg.CreateFreeMsg(mappedValues))
                processor.Post(Msg.CreateFreeMsg(indices))
                processor.Post(Msg.CreateFreeMsg(values))

                ClVector.Dense result

    let copy (clContext: ClContext) workGroupSize =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        let copyOptionData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector ->
                let vector =
                    { Context = clContext
                      Indices = copy processor flag vector.Indices
                      Values = copyData processor flag vector.Values
                      Size = vector.Size }

                ClVector.Sparse vector
            | ClVector.Dense vector ->
                ClVector.Dense
                <| copyOptionData processor flag vector

    let mask = copy

    let toSparse (clContext: ClContext) workGroupSize =
        let toSparse =
            DenseVector.toSparse clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector -> ClVector.Sparse <| toSparse processor flag vector
            | ClVector.Sparse _ -> copy processor flag vector

    let toDense (clContext: ClContext) workGroupSize =
        let toDense =
            SparseVector.toDense clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector -> ClVector.Dense <| copy processor flag vector
            | ClVector.Sparse vector -> ClVector.Dense <| toDense processor flag vector

    let elementWiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let addSparse =
            SparseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        let addDense =
            DenseVector.elementWiseAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| addSparse processor flag left right
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| addDense processor flag left right
            | _ -> failwith "Vector formats are not matching."

    let elementWise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let addDense =
            DenseVector.elementWise clContext opAdd workGroupSize

        let addSparse =
            SparseVector.elementWise clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| addDense processor flag left right
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| addSparse processor flag left right
            | _ -> failwith "Vector formats are not matching."

    let elementwiseGeneral<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupsSize
        =

        let sparseEWise =
            SparseVector.elementwiseGeneral clContext opAdd workGroupsSize

        let denseEWise =
            DenseVector.elementWise clContext opAdd workGroupsSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right ->
                ClVector.Sparse
                <| sparseEWise processor flag left right
            | ClVector.Dense left, ClVector.Dense right ->
                ClVector.Dense
                <| denseEWise processor flag left right
            | _ -> failwith "Vector formats are not matching."

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> maskOp (clContext: ClContext) workGroupSize =
        let sparseFillVector =
            SparseVector.fillSubVector clContext (Convert.fillSubToOption maskOp) workGroupSize

        let denseFillVector =
            DenseVector.fillSubVector clContext (Convert.fillSubToOption maskOp) workGroupSize

        let toSparseVector =
            DenseVector.toSparse clContext workGroupSize

        let toSparseMask =
            DenseVector.toSparse clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (vector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match vector, maskVector with
            | ClVector.Sparse vector, ClVector.Sparse mask ->
                ClVector.Sparse
                <| sparseFillVector processor flag vector mask value
            | ClVector.Sparse vector, ClVector.Dense mask ->
                let mask = toSparseMask processor flag mask

                ClVector.Sparse
                <| sparseFillVector processor flag vector mask value
            | ClVector.Dense vector, ClVector.Sparse mask ->
                let vector = toSparseVector processor flag vector

                ClVector.Sparse
                <| sparseFillVector processor flag vector mask value
            | ClVector.Dense vector, ClVector.Dense mask ->
                ClVector.Dense
                <| denseFillVector processor flag vector mask value

    let fillSubVectorComplemented<'a, 'b when 'a: struct and 'b: struct> maskOp (clContext: ClContext) workGroupSize =

        let denseFillVector =
            DenseVector.fillSubVector clContext (Convert.fillSubComplementedToOption maskOp) workGroupSize

        let vectorToDense =
            SparseVector.toDense clContext workGroupSize

        let maskToDense =
            SparseVector.toDense clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector<'a>) (maskVector: ClVector<'b>) (value: ClCell<'a>) ->
            match leftVector, maskVector with
            | ClVector.Sparse vector, ClVector.Sparse mask ->
                let denseVector = vectorToDense processor flag vector
                let denseMask = maskToDense processor flag mask

                ClVector.Dense
                <| denseFillVector processor flag denseVector denseMask value
            | ClVector.Dense vector, ClVector.Sparse mask ->
                let denseMask = maskToDense processor flag mask

                ClVector.Dense
                <| denseFillVector processor flag vector denseMask value
            | ClVector.Sparse vector, ClVector.Dense mask ->
                let denseVector = vectorToDense processor flag vector

                ClVector.Dense
                <| denseFillVector processor flag denseVector mask value
            | ClVector.Dense vector, ClVector.Dense mask ->
                ClVector.Dense
                <| denseFillVector processor flag vector mask value

    let standardFillSubVector<'a, 'b when 'a: struct and 'b: struct> = fillSubVector<'a, 'b> Mask.fillSubOp<'a>

    let standardFillSubVectorComplemented<'a, 'b when 'a: struct and 'b: struct> =
        fillSubVectorComplemented<'a, 'b> Mask.fillSubOp<'a>

    let reduce (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =
        let sparseReduce =
            SparseVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector -> sparseReduce processor vector
            | ClVector.Dense vector -> denseReduce processor vector
