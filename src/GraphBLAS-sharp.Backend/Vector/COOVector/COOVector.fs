namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Control

module COOVector =
    let zeroCreate (clContext: ClContext) =
        let resultIndices = clContext.CreateClArray [||]
        let resultValues = clContext.CreateClArray [||]

        { ClCooVector.Context = clContext
          Indices = resultIndices
          Values = resultValues
          Size = 0 }

    let ofList (clContext: ClContext) (elements: (int * 'a) list) =
        let (indices, values) =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let resultSize = elements.Length

        let resultIndices = clContext.CreateClArray indices
        let resultValues = clContext.CreateClArray values

        { ClCooVector.Context = clContext
          Indices = resultIndices
          Values = resultValues
          Size = resultSize }

    let mask (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClCooVector<'a>) ->
            let resultIndices = copy processor vector.Indices

            let resultValues = copyData processor vector.Values

            let resultSize = vector.Size

            { ClCooVector.Context = clContext
              Indices = resultIndices
              Values = resultValues
              Size = resultSize }

    (*let fillSubVector (clContext: ClContext) (workGroupSize: int) =

        fun (processor: MailboxProcessor<_>) (leftVector: ClCooVector<'a>) (mask: ClVector<'b>) (scalar: 'c) ->*)

