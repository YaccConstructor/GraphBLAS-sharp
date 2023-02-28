namespace GraphBLAS.FSharp.Backend.Matrix

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell

module Common =
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

        let valuesScatter =
            Scatter.runInplace clContext workGroupSize

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

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
