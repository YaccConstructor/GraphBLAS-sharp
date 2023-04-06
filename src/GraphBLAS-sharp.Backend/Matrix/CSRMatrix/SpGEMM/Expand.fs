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

type Indices = ClArray<int>

type Values<'a> = ClArray<'a>

module Expand =
    let getSegmentPointers (clContext: ClContext) workGroupSize =

        let subtract = ClArray.map2 clContext workGroupSize Map.subtraction

        let idGather = Gather.runInit Map.id clContext workGroupSize

        let incGather = Gather.runInit Map.inc clContext workGroupSize

        let gather = Gather.run clContext workGroupSize

        let prefixSum = PrefixSum.standardExcludeInplace clContext workGroupSize

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
            let length = (prefixSum processor segmentsLengths).ToHostAndFree processor

            length, segmentsLengths

    let expand (clContext: ClContext) workGroupSize opMul =

        let init = ClArray.init clContext workGroupSize Map.id

        let scatter = Scatter.lastOccurrence clContext workGroupSize

        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        let maxPrefixSum = PrefixSum.runIncludeInplace <@ max @> clContext workGroupSize

        let create = ClArray.create clContext workGroupSize

        let gather = Gather.run clContext workGroupSize

        let segmentPrefixSum = PrefixSum.ByKey.sequentialInclude clContext workGroupSize <@ (+) @> 0

        let removeDuplicates = ClArray.removeDuplications clContext workGroupSize

        let expandRowPointers = Common.expandRowPointers clContext workGroupSize

        let AGather = Gather.run clContext workGroupSize

        let BGather = Gather.run clContext workGroupSize

        let mul = ClArray.map2 clContext workGroupSize opMul

        fun (processor: MailboxProcessor<_>) lengths (segmentsPointers: Indices) (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            // Compute A positions
            let sequence = init processor DeviceOnly segmentsPointers.Length // TODO(fuse)

            let APositions = zeroCreate processor DeviceOnly lengths

            scatter processor segmentsPointers sequence APositions

            sequence.Free processor

            (maxPrefixSum processor APositions 0).Free processor

            // Compute B positions
            let BPositions = create processor DeviceOnly lengths 1 // TODO(fuse)

            let requiredBPointers = zeroCreate processor DeviceOnly leftMatrix.Columns.Length

            gather processor leftMatrix.Columns rightMatrix.RowPointers requiredBPointers

            scatter processor segmentsPointers requiredBPointers BPositions

            requiredBPointers.Free processor

            // another way to get offsets ???
            let offsets = removeDuplicates processor segmentsPointers

            segmentPrefixSum processor offsets.Length BPositions APositions offsets

            offsets.Free processor

            // compute columns
            let columns =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            gather processor BPositions rightMatrix.Columns columns

            // compute rows
            let ARows = expandRowPointers processor DeviceOnly leftMatrix.RowPointers leftMatrix.NNZ leftMatrix.RowCount

            let rows =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            gather processor APositions ARows rows

            ARows.Free processor

            // compute leftMatrix values
            let AValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            AGather processor APositions leftMatrix.Values AValues

            APositions.Free processor

            // compute right matrix values
            let BValues =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            BGather processor BPositions rightMatrix.Values BValues

            BPositions.Free processor

            // multiply values TODO(filter values)
            let values = mul processor DeviceOnly AValues BValues

            AValues.Free processor
            BValues.Free processor

            values, columns, rows

    let sortByColumnsAndRows (clContext: ClContext) workGroupSize =

        let sortByKeyIndices = Radix.runByKeysStandard clContext workGroupSize

        let sortByKeyValues = Radix.runByKeysStandard clContext workGroupSize

        let sortKeys = Radix.standardRunKeysOnly clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (columns: Indices) (rows: Indices) ->
            // sort by columns
            let valuesSortedByColumns = sortByKeyValues processor DeviceOnly columns values

            let rowsSortedByColumns = sortByKeyIndices processor DeviceOnly columns rows

            let sortedColumns = sortKeys processor columns

            // sort by rows
            let valuesSortedByRows = sortByKeyValues processor DeviceOnly rowsSortedByColumns valuesSortedByColumns

            let columnsSortedByRows = sortByKeyIndices processor DeviceOnly rowsSortedByColumns sortedColumns

            let sortedRows = sortKeys processor rowsSortedByColumns

            valuesSortedByColumns.Free processor
            rowsSortedByColumns.Free processor
            sortedColumns.Free processor

            valuesSortedByRows, columnsSortedByRows, sortedRows

    let reduce (clContext: ClContext) workGroupSize opAdd  =

        let reduce = Reduce.ByKey2D.segmentSequential clContext workGroupSize opAdd

        let getUniqueBitmap =
            ClArray.getUniqueBitmap2LastOccurrence clContext workGroupSize

        let prefixSum = PrefixSum.standardExcludeInplace clContext workGroupSize

        let init = ClArray.init clContext workGroupSize Map.id // TODO(fuse)

        let scatter = Scatter.firstOccurrence clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (values: ClArray<'a>) (columns: Indices) (rows: Indices) ->

            let bitmap = getUniqueBitmap processor DeviceOnly columns rows

            printfn $"key bitmap: %A{bitmap.ToHost processor}"

            let uniqueKeysCount = (prefixSum processor bitmap).ToHostAndFree processor

            printfn $"key bitmap after prefix sum: %A{bitmap.ToHost processor}"

            let positions = init processor DeviceOnly bitmap.Length

            printfn $"positions: %A{positions.ToHost processor}"

            let offsets = clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, uniqueKeysCount)

            scatter processor bitmap positions offsets

            printfn $"offsets: %A{offsets.ToHost processor}"

            bitmap.Free processor
            positions.Free processor

            let reducedColumns, reducedRows, reducedValues = // by size variance TODO()
                reduce processor allocationMode uniqueKeysCount offsets columns rows values

            offsets.Free processor

            reducedValues, reducedColumns, reducedRows

    let run (clContext: ClContext) workGroupSize opAdd opMul =

        let getSegmentPointers = getSegmentPointers clContext workGroupSize

        let expand = expand clContext workGroupSize opMul

        let sort = sortByColumnsAndRows clContext workGroupSize

        let reduce = reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let length, segmentPointers = getSegmentPointers processor leftMatrix rightMatrix

            let values, columns, rows =
                expand processor length segmentPointers leftMatrix rightMatrix

            printfn $"expanded values: %A{values.ToHost processor}"
            printfn $"expanded columns: %A{columns.ToHost processor}"
            printfn $"expanded rows: %A{rows.ToHost processor}"

            let sortedValues, sortedColumns, sortedRows =
                sort processor values columns rows

            printfn $"sorted values: %A{sortedValues.ToHost processor}"
            printfn $"sorted columns: %A{sortedColumns.ToHost processor}"
            printfn $"sorted rows: %A{sortedRows.ToHost processor}"

            values.Free processor
            columns.Free processor
            rows.Free processor

            let reducedValues, reducedColumns, reducedRows =
                reduce processor allocationMode sortedValues sortedColumns sortedRows

            printfn $"reduced values: %A{reducedValues.ToHost processor}"
            printfn $"reduced columns: %A{reducedColumns.ToHost processor}"
            printfn $"reduced rows: %A{reducedRows.ToHost processor}"

            sortedValues.Free processor
            sortedColumns.Free processor
            sortedRows.Free processor

            reducedValues, reducedColumns, reducedRows

