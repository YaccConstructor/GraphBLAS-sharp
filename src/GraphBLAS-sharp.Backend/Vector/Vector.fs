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
    let zeroCreate (clContext: ClContext) workGroupSize flag =
        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) size format ->
            match format with
            | Sparse ->
                ClVector.Sparse
                    { Context = clContext
                      Indices = clContext.CreateClArrayWithFlag(flag, [| 0 |])
                      Values = clContext.CreateClArrayWithFlag(flag, [| Unchecked.defaultof<'a> |])
                      Size = size }
            | Dense -> ClVector.Dense <| zeroCreate processor size

    let ofList (clContext: ClContext) workGroupSize flag =
        let scatter =
            Scatter.runInplace clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize flag

        let map =
            ClArray.map clContext workGroupSize DeviceOnly <@ Some @>

        fun (processor: MailboxProcessor<_>) format size (elements: (int * 'a) list) ->
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

                let mappedValues = map processor values

                let result = zeroCreate processor size

                scatter processor indices mappedValues result

                processor.Post(Msg.CreateFreeMsg(mappedValues))
                processor.Post(Msg.CreateFreeMsg(indices))
                processor.Post(Msg.CreateFreeMsg(values))

                ClVector.Dense result

    let copy (clContext: ClContext) workGroupSize flag =
        let copy =
            ClArray.copy clContext workGroupSize flag

        let copyData =
            ClArray.copy clContext workGroupSize flag

        let copyOptionData =
            ClArray.copy clContext workGroupSize flag

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

    let toSparse (clContext: ClContext) workGroupSize flag =
        let toSparse =
            DenseVector.toSparse clContext workGroupSize flag

        let copy = copy clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector -> ClVector.Sparse <| toSparse processor vector
            | ClVector.Sparse _ -> copy processor vector

    let toDense (clContext: ClContext) workGroupSize flag =
        let toDense =
            SparseVector.toDense clContext workGroupSize flag

        let copy =
            ClArray.copy clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Dense vector -> ClVector.Dense <| copy processor vector
            | ClVector.Sparse vector -> ClVector.Dense <| toDense processor vector

    let elementWiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize flag =
        let addSparse =
            SparseVector.elementWiseAtLeastOne clContext opAdd workGroupSize flag

        let addDense =
            DenseVector.elementWiseAtLeastOne clContext opAdd workGroupSize flag

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Sparse left, ClVector.Sparse right -> ClVector.Sparse <| addSparse processor left right
            | ClVector.Dense left, ClVector.Dense right -> ClVector.Dense <| addDense processor left right
            | _ -> failwith "Vector formats are not matching."

    let elementWise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize flag =
        let addDense =
            DenseVector.elementWise clContext opAdd workGroupSize flag

        let addSparse =
            SparseVector.elementWise clContext opAdd workGroupSize flag

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVector.Dense leftVector, ClVector.Dense rightVector ->
                ClVector.Dense
                <| addDense processor leftVector rightVector
            | ClVector.Sparse left, ClVector.Sparse right -> ClVector.Sparse <| addSparse processor left right
            | _ -> failwith "Vector formats are not matching."

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> maskOp (clContext: ClContext) workGroupSize flag =
        let sparseFillVector =
            SparseVector.fillSubVector clContext (Convert.fillSubToOption maskOp) workGroupSize flag

        let denseFillVector =
            DenseVector.fillSubVector clContext (Convert.fillSubToOption maskOp) workGroupSize flag

        let toSparseVector =
            DenseVector.toSparse clContext workGroupSize flag

        let toSparseMask =
            DenseVector.toSparse clContext workGroupSize flag

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
        workGroupSize
        flag
        =

        let denseFillVector =
            DenseVector.fillSubVector clContext (Convert.fillSubComplementedToOption maskOp) workGroupSize flag

        let vectorToDense =
            SparseVector.toDense clContext workGroupSize flag

        let maskToDense =
            SparseVector.toDense clContext workGroupSize flag

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

    let reduce (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =
        let sparseReduce =
            SparseVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVector.Sparse vector -> sparseReduce processor vector
            | ClVector.Dense vector -> denseReduce processor vector
