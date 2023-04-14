namespace GraphBLAS.FSharp.Backend.Matrix.CSR.SpGeMM

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open FSharp.Quotations

type Indices = ClArray<int>

type Values<'a> = ClArray<'a>

module Expand =
    let getSegmentPointers (clContext: ClContext) workGroupSize =

        let subtract =
            ClArray.map2 clContext workGroupSize Map.subtraction

        let idGather =
            Gather.runInit Map.id clContext workGroupSize

        let incGather =
            Gather.runInit Map.inc clContext workGroupSize

        let gather = Gather.run clContext workGroupSize

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let positionsLength = rightMatrix.RowPointers.Length - 1

            // extract first rightMatrix.RowPointers.Lengths - 1 indices from rightMatrix.RowPointers
            // (right matrix row pointers without last item)
            let firstPointers =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, positionsLength)

            idGather processor rightMatrix.RowPointers firstPointers

            // extract last rightMatrix.RowPointers.Lengths - 1 indices from rightMatrix.RowPointers
            // (right matrix row pointers without first item)
            let lastPointers =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, positionsLength)

            incGather processor rightMatrix.RowPointers lastPointers

            // subtract
            let rightMatrixRowsLengths =
                subtract processor DeviceOnly lastPointers firstPointers

            firstPointers.Free processor
            lastPointers.Free processor

            let segmentsLengths =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftMatrix.Columns.Length)

            // extract needed lengths by left matrix nnz
            gather processor leftMatrix.Columns rightMatrixRowsLengths segmentsLengths

            rightMatrixRowsLengths.Free processor

            // compute pointers
            let length =
                (prefixSum processor segmentsLengths)
                    .ToHostAndFree processor

            length, segmentsLengths

    let multiply (clContext: ClContext) workGroupSize (predicate: Expr<'a -> 'b -> 'c option>) =
        let getBitmap =
            ClArray.map2<'a, 'b, int> clContext workGroupSize
            <| Map.choose2Bitmap predicate

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let assignValues =
            ClArray.assignOption2 clContext workGroupSize predicate

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (firstValues: ClArray<'a>) (secondValues: ClArray<'b>) (columns: Indices) (rows: Indices) ->

            let positions =
                getBitmap processor DeviceOnly firstValues secondValues

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            let resultColumns =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            scatter processor positions columns resultColumns

            let resultRows =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            scatter processor positions rows resultRows

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            assignValues processor firstValues secondValues positions resultValues

            resultValues, resultColumns, resultRows

    let expand (clContext: ClContext) workGroupSize =

        let idScatter =
            Scatter.initLastOccurrence Map.id clContext workGroupSize

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let maxPrefixSum =
            PrefixSum.runIncludeInplace <@ max @> clContext workGroupSize

        let create = ClArray.create clContext workGroupSize

        let gather = Gather.run clContext workGroupSize

        let segmentPrefixSum =
            PrefixSum.ByKey.sequentialInclude clContext workGroupSize <@ (+) @> 0

        let removeDuplicates =
            ClArray.removeDuplications clContext workGroupSize

        let expandRowPointers =
            Common.expandRowPointers clContext workGroupSize

        let leftMatrixGather = Gather.run clContext workGroupSize

        let rightMatrixGather = Gather.run clContext workGroupSize

        fun (processor: MailboxProcessor<_>) lengths (segmentsPointers: Indices) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            // Compute left matrix positions
            let leftMatrixPositions = zeroCreate processor DeviceOnly lengths

            idScatter processor segmentsPointers leftMatrixPositions

            (maxPrefixSum processor leftMatrixPositions 0)
                .Free processor

            // Compute right matrix positions
            let rightMatrixPositions = create processor DeviceOnly lengths 1

            let requiredRightMatrixPointers =
                zeroCreate processor DeviceOnly leftMatrix.Columns.Length

            gather processor leftMatrix.Columns rightMatrix.RowPointers requiredRightMatrixPointers

            scatter processor segmentsPointers requiredRightMatrixPointers rightMatrixPositions

            requiredRightMatrixPointers.Free processor

            // another way to get offsets ???
            let offsets =
                removeDuplicates processor segmentsPointers

            segmentPrefixSum processor offsets.Length rightMatrixPositions leftMatrixPositions offsets

            offsets.Free processor

            // compute columns
            let columns =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            gather processor rightMatrixPositions rightMatrix.Columns columns

            // compute rows
            let leftMatrixRows =
                expandRowPointers processor DeviceOnly leftMatrix.RowPointers leftMatrix.NNZ leftMatrix.RowCount

            let rows =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            gather processor leftMatrixPositions leftMatrixRows rows

            leftMatrixRows.Free processor

            // compute left matrix values
            let leftMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            leftMatrixGather processor leftMatrixPositions leftMatrix.Values leftMatrixValues

            leftMatrixPositions.Free processor

            // compute right matrix values
            let rightMatrixValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            rightMatrixGather processor rightMatrixPositions rightMatrix.Values rightMatrixValues

            rightMatrixPositions.Free processor

            // left, right matrix values, columns and rows indices
            leftMatrixValues, rightMatrixValues, columns, rows

    let sortByColumnsAndRows (clContext: ClContext) workGroupSize =

        let sortByKeyIndices =
            Radix.runByKeysStandard clContext workGroupSize

        let sortByKeyValues =
            Radix.runByKeysStandard clContext workGroupSize

        let sortKeys =
            Radix.standardRunKeysOnly clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (columns: Indices) (rows: Indices) ->
            // sort by columns
            let valuesSortedByColumns =
                sortByKeyValues processor DeviceOnly columns values

            let rowsSortedByColumns =
                sortByKeyIndices processor DeviceOnly columns rows

            let sortedColumns = sortKeys processor columns

            // sort by rows
            let valuesSortedByRows =
                sortByKeyValues processor DeviceOnly rowsSortedByColumns valuesSortedByColumns

            let columnsSortedByRows =
                sortByKeyIndices processor DeviceOnly rowsSortedByColumns sortedColumns

            let sortedRows = sortKeys processor rowsSortedByColumns

            valuesSortedByColumns.Free processor
            rowsSortedByColumns.Free processor
            sortedColumns.Free processor

            valuesSortedByRows, columnsSortedByRows, sortedRows

    let reduce (clContext: ClContext) workGroupSize opAdd =

        let reduce =
            Reduce.ByKey2D.segmentSequentialOption clContext workGroupSize opAdd

        let getUniqueBitmap =
            ClArray.getUniqueBitmap2LastOccurrence clContext workGroupSize

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let idScatter =
            Scatter.initFirsOccurrence Map.id clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (values: ClArray<'a>) (columns: Indices) (rows: Indices) ->

            let bitmap =
                getUniqueBitmap processor DeviceOnly columns rows

            let uniqueKeysCount =
                (prefixSum processor bitmap)
                    .ToHostAndFree processor

            let offsets =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, uniqueKeysCount)

            idScatter processor bitmap offsets

            bitmap.Free processor

            let reducedColumns, reducedRows, reducedValues = // by size variance TODO()
                reduce processor allocationMode uniqueKeysCount offsets columns rows values

            offsets.Free processor

            reducedValues, reducedColumns, reducedRows

    let run (clContext: ClContext) workGroupSize opAdd opMul =

        let getSegmentPointers =
            getSegmentPointers clContext workGroupSize

        let expand = expand clContext workGroupSize

        let multiply = multiply clContext workGroupSize opMul

        let sort =
            sortByColumnsAndRows clContext workGroupSize

        let reduce = reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let length, segmentPointers =
                getSegmentPointers processor leftMatrix rightMatrix

            // expand
            let leftMatrixValues, rightMatrixValues, columns, rows =
                expand processor length segmentPointers leftMatrix rightMatrix

            // multiply
            let resultValues, resultColumns, resultRows =
                multiply processor leftMatrixValues rightMatrixValues columns rows

            leftMatrixValues.Free processor
            rightMatrixValues.Free processor
            columns.Free processor
            rows.Free processor

            // sort
            let sortedValues, sortedColumns, sortedRows =
                sort processor resultValues resultColumns resultRows

            resultValues.Free processor
            resultColumns.Free processor
            resultRows.Free processor

            // addition
            let reducedValues, reducedColumns, reducedRows =
                reduce processor allocationMode sortedValues sortedColumns sortedRows

            sortedValues.Free processor
            sortedColumns.Free processor
            sortedRows.Free processor

            reducedValues, reducedColumns, reducedRows
