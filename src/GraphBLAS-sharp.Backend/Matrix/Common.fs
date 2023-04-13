namespace GraphBLAS.FSharp.Backend.Matrix

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell

module Common =
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let setPositionsUnsafe<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let indicesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        let valuesScatter =
            Scatter.lastOccurrence clContext workGroupSize

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

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let setPositionsSafe<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let indicesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        let valuesScatter =
            Scatter.lastOccurrence clContext workGroupSize

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

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

    let expandRowPointers (clContext: ClContext) workGroupSize =

        let expandRowPointers =
            <@ fun (ndRange: Range1D) (rowPointers: ClArray<int>) (rowCount: int) (rows: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < rowCount then
                    let rowPointer = rowPointers.[i]

                    if rowPointer <> rowPointers.[i + 1] then
                        rows.[rowPointer] <- i @>

        let program = clContext.Compile expandRowPointers

        let create =
            ClArray.zeroCreate clContext workGroupSize

        let scan =
            PrefixSum.runIncludeInplace <@ max @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rowPointers: ClArray<int>) nnz rowCount ->

            let rows = create processor allocationMode nnz

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(rowCount, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowPointers rowCount rows))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            (scan processor rows 0).Free processor

            rows
