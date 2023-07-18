namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClVector
open Microsoft.FSharp.Control
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCellExtensions

module internal Common =
    let setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let sum =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        let valuesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        let indicesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (allValues: ClArray<'a>) (allIndices: ClArray<int>) (positions: ClArray<int>) ->

            let resultLength =
                (sum processor positions).ToHostAndFree(processor)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(allocationMode, resultLength)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

            valuesScatter processor positions allValues resultValues

            indicesScatter processor positions allIndices resultIndices

            resultValues, resultIndices

    let setPositionsOption<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let sum =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        let valuesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        let indicesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (allValues: ClArray<'a>) (allIndices: ClArray<int>) (positions: ClArray<int>) ->

            let resultLength =
                (sum processor positions).ToHostAndFree(processor)

            if resultLength = 0 then
                None
            else
                let resultValues =
                    clContext.CreateClArrayWithSpecificAllocationMode<'a>(allocationMode, resultLength)

                let resultIndices =
                    clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

                valuesScatter processor positions allValues resultValues

                indicesScatter processor positions allIndices resultIndices

                (resultValues, resultIndices) |> Some

    let concat (clContext: ClContext) workGroupSize =

        let concatValues = ClArray.concat clContext workGroupSize

        let concatIndices = ClArray.concat clContext workGroupSize

        let mapIndices =
            ClArray.mapWithValue clContext workGroupSize <@ fun x y -> x + y @>

        fun (processor: MailboxProcessor<_>) allocationMode (vectors: Sparse<'a> seq) ->

            let vectorIndices, _ =
                vectors
                |> Seq.mapFold
                    (fun offset vector ->
                        let newIndices =
                            mapIndices processor allocationMode offset vector.Indices

                        newIndices, offset + vector.Size)
                    0

            let vectorValues =
                vectors |> Seq.map (fun vector -> vector.Values)

            let resultIndices =
                concatIndices processor allocationMode vectorIndices

            let resultValues =
                concatValues processor allocationMode vectorValues

            resultIndices, resultValues
