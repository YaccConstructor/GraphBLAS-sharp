namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations

module Vector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int) =

        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (size: int) (format: VectorFormat) ->
            match format with
            | COO ->
                let indices = clContext.CreateClArray [||]
                let values = clContext.CreateClArray [||]

                let vector =
                    { ClCooVector.Context = clContext
                      Indices = indices
                      Values = values
                      Size = size }

                ClVectorCOO vector
            | Dense ->
                let resultValues = zeroCreate processor size
                let vector = resultValues :?> ClDenseVector<'a>

                ClVectorDense vector

    let ofList (clContext: ClContext) (workGroupSize: int) (elements: (int * 'a) list) =

        let toOptionArray = ClArray.toOptionArray clContext workGroupSize

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let indices = clContext.CreateClArray indices
        let values = clContext.CreateClArray values

        fun (processor: MailboxProcessor<_>) (format: VectorFormat) ->
            match format with
            | COO ->
                let resultSize = elements.Length

                let vector =
                  { ClCooVector.Context = clContext
                    Indices = indices
                    Values = values
                    Size = resultSize }

                ClVectorCOO vector

            | Dense ->
                let array =
                    toOptionArray processor values indices elements.Length

                let vector =
                    array :?> ClDenseVector<'a>

                ClVectorDense vector

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
               let vector =
                   copyOptionData processor vector :?> ClDenseVector<'a>

               ClVectorDense vector

    let mask = copy

    let fillSubVector (clContext: ClContext) (workGroupSize: int) =
        let cooFillVector =
            COOVector.fillSubVector clContext workGroupSize

        let denseFillVector =
            DenseVector.fillSubVector clContext workGroupSize

        let toCooVector =
            DenseVector.toCoo clContext workGroupSize

        let toCooMask =
            DenseVector.toCoo clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) (maskVector: ClVector<'b>) (value: 'a) -> //TODO()
            match vector, maskVector with
            | ClVectorCOO vector, ClVectorCOO mask ->
                let res = cooFillVector processor vector mask value

                ClVectorCOO res
            | ClVectorCOO vector, ClVectorDense mask ->
                let mask = toCooMask processor mask

                let res = cooFillVector processor vector mask value //TODO()

                ClVectorCOO res
            | ClVectorDense vector, ClVectorCOO mask ->
                let vector = toCooVector processor vector

                let res =
                    cooFillVector processor vector mask value //TODO()

                ClVectorCOO res
            | ClVectorDense vector, ClVectorDense mask ->
                let res = denseFillVector processor vector mask value //TODO()

                ClVectorDense res

    let complemented<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =
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

    let reduce (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =
        let cooReduce =
            COOVector.reduce clContext workGroupSize opAdd

        let denseReduce =
            DenseVector.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector<'a>) ->
            match vector with
            | ClVectorCOO vector ->
                cooReduce processor vector
            | ClVectorDense vector ->
                denseReduce processor vector
