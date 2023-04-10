namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Control
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell

module internal Common =
    let setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let valuesScatter =
            Scatter.runInplace clContext workGroupSize

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

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

    let setPositionsNotEmpty<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let valuesScatter =
            Scatter.runInplace clContext workGroupSize

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (allValues: ClArray<'a>) (allIndices: ClArray<int>) (positions: ClArray<int>) ->

            let resultLength =
                (sum processor positions).ToHostAndFree(processor)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(
                    allocationMode,
                    if resultLength > 0 then
                        resultLength
                    else
                        1
                )

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(
                    allocationMode,
                    if resultLength > 0 then
                        resultLength
                    else
                        1
                )

            if resultLength > 0 then
                valuesScatter processor positions allValues resultValues

                indicesScatter processor positions allIndices resultIndices

            resultValues, resultIndices, resultLength
