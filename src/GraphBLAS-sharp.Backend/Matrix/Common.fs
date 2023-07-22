namespace GraphBLAS.FSharp.Backend.Matrix

open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

module internal Common =
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let indicesScatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let valuesScatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let sum =
            Common.PrefixSum.standardExcludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->

            let resultLength =
                (sum processor positions).ToHostAndFree(processor)

            let resultRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

            let resultColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            indicesScatter processor positions allRows resultRows

            indicesScatter processor positions allColumns resultColumns

            valuesScatter processor positions allValues resultValues

            resultRows, resultColumns, resultValues, resultLength

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let setPositionsOption<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let indicesScatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let valuesScatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let sum =
            Common.PrefixSum.standardExcludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->

            let resultLength =
                (sum processor positions).ToHostAndFree(processor)

            if resultLength = 0 then
                None
            else
                let resultRows =
                    clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

                let resultColumns =
                    clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

                let resultValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                indicesScatter processor positions allRows resultRows

                indicesScatter processor positions allColumns resultColumns

                valuesScatter processor positions allValues resultValues

                Some(resultRows, resultColumns, resultValues, resultLength)
