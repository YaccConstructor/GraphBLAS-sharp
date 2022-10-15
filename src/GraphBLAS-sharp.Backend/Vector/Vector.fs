namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open Microsoft.FSharp.Quotations

module Vector =
    let copy (clContext: ClContext) (workGroupSize: int) =
        fun (processor: MailboxProcessor<_>) vector ->
            match vector with
            | ClVectorCOO vector ->
                let res = COOVector.copy clContext workGroupSize processor vector

                ClVectorCOO res
            | ClVectorDense vector ->
                let res = DenseVector.copy clContext workGroupSize processor vector

                ClVectorDense res

    let mask (clContext: ClContext) (workGroupSize: int)  =
        copy clContext workGroupSize

    let fillSubVector (clContext: ClContext) (workGroupSize: int) =

        let cooFillVector = COOVector.fillSubVector clContext workGroupSize
        let denseFillVector = DenseVector.fillSubVector clContext workGroupSize

        let toCooVector = DenseVector.toCoo clContext workGroupSize
        let toCooMask = DenseVector.toCoo clContext workGroupSize

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

                let res = cooFillVector processor vector mask value //TODO()

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






