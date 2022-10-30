namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

module Vector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int) =

        let denseZeroCreate = ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (size: int) (format: VectorFormat) ->
            match format with
            | COO ->
                let vector =
                    { ClCooVector.Context = clContext
                      Indices = clContext.CreateClArray<int> [| 0 |]
                      Values = clContext.CreateClArray<'a> [| Unchecked.defaultof<'a> |]
                      Size = 0 }

                ClVectorCOO vector
            | Dense ->
                ClVectorDense <| denseZeroCreate processor size

    let ofList (clContext: ClContext) (workGroupSize: int) (elements: (int * 'a) list) =

        let toOptionArray = ClArray.toOptionArray clContext workGroupSize

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let clIndices = clContext.CreateClArray indices
        let clValues = clContext.CreateClArray values

        let resultLenght = (Array.max indices) + 1

        fun (processor: MailboxProcessor<_>) (format: VectorFormat) ->
            match format with
            | COO ->
                let vector =
                  { ClCooVector.Context = clContext
                    Indices = clIndices
                    Values = clValues
                    Size = resultLenght }

                ClVectorCOO vector
            | Dense ->
                ClVectorDense
                <| toOptionArray processor clValues clIndices resultLenght

    let copy (clContext: ClContext) (workGroupSize: int) =
       let copy =
           ClArray.copy clContext workGroupSize

       let copyData =
            ClArray.copy clContext workGroupSize

       let copyOptionData =
           ClArray.copy clContext workGroupSize

       fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
           match vector with
           | ClVectorCOO vector ->
               let vector =
                 { ClCooVector.Context = clContext
                   Indices = copy processor vector.Indices
                   Values = copyData processor vector.Values
                   Size = vector.Size }

               ClVectorCOO vector
           | ClVectorDense vector ->
               ClVectorDense <| copyOptionData processor vector

    let mask = copy

    let toCoo (clContext: ClContext) (workGroupSize: int) =
        let toCoo = DenseVector.toCoo clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorDense vector ->
                ClVectorCOO <| toCoo processor vector
            | ClVectorCOO _ ->
                copy processor vector

    let elementWiseAddAtLeastOne
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        let addCoo =
            COOVector.elementWiseAddAtLeastOne clContext opAdd workGroupSize

        let addDense =
            DenseVector.elementWiseAddAtLeasOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
            match leftVector, rightVector with
            | ClVectorCOO left, ClVectorCOO right ->
                ClVectorCOO <| addCoo processor left right
            | ClVectorDense left, ClVectorDense right ->
                ClVectorDense <| addDense processor left right
            | _ -> failwith "Vector formats are not matching."

    let fillSubVector (clContext: ClContext) (workGroupSize: int) = //TODO() remove zero
        let cooFillVector =
            COOVector.fillSubVector clContext workGroupSize

        let denseFillVector =
            DenseVector.fillSubVector clContext workGroupSize

        let toCooVector =
            DenseVector.toCoo clContext workGroupSize

        let toCooMask =
            DenseVector.toCoo clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) (maskVector: ClVector<'b>) (value: 'a) ->
            match vector, maskVector with
            | ClVectorCOO vector, ClVectorCOO mask ->
                ClVectorCOO <| cooFillVector processor vector mask value
            | ClVectorCOO vector, ClVectorDense mask ->
                let mask = toCooMask processor mask

                ClVectorCOO <| cooFillVector processor vector mask value
            | ClVectorDense vector, ClVectorCOO mask ->
                let vector = toCooVector processor vector

                ClVectorCOO <| cooFillVector processor vector mask value
            | ClVectorDense vector, ClVectorDense mask ->
                ClVectorDense <| denseFillVector processor vector mask value

    let complemented (clContext: ClContext) (workGroupSize: int) =
        let cooComplemented =
            COOVector.complemented clContext workGroupSize

        let denseComplemented =
            DenseVector.complemented clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorCOO vector ->
                ClVectorCOO <| cooComplemented processor vector
            | ClVectorDense vector ->
                ClVectorDense <| denseComplemented processor vector

    let reduce (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) (zero: 'a) =
        let cooReduce =
            COOVector.reduce clContext workGroupSize opAdd zero

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd zero

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorCOO vector ->
                cooReduce processor vector
            | ClVectorDense vector ->
                denseReduce processor vector
