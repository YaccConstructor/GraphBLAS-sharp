namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Control

module DenseVector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int)  =
        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (length: int) ->
            let resultValues = zeroCreate processor length

            resultValues :?> ClDenseVector<'a>

    let ofList (clContext: ClContext) (workGroupSize: int) (elements: (int * 'a) list) =
        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let toOptionArray = ClArray.toOptionArray clContext workGroupSize

        fun (processor: MailboxProcessor<_>) ->
            let values = clContext.CreateClArray values
            let indices = clContext.CreateClArray indices

            toOptionArray processor values indices elements.Length :?> ClDenseVector<'a>


    (*let mask (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->
            copy processor vector :?> ClDenseVector<'a>*)

