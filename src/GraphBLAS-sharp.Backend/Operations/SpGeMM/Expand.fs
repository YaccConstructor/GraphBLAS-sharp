namespace GraphBLAS.FSharp.Backend.Operations.SpGeMM

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes

module internal Expand =
    let getSegmentPointers (clContext: ClContext) workGroupSize =

        let gather =
            Common.Gather.run clContext workGroupSize

        let prefixSum =
            Common.PrefixSum.standardExcludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrixColumns: ClArray<int>) (rightMatrixRowsLengths: ClArray<int>) ->

            let segmentsLengths =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftMatrixColumns.Length)

            // extract needed lengths by left matrix nnz
            gather processor leftMatrixColumns rightMatrixRowsLengths segmentsLengths

            // compute pointers
            let length =
                (prefixSum processor segmentsLengths)
                    .ToHostAndFree processor

            length, segmentsLengths

    let multiply (predicate: Expr<'a -> 'b -> 'c option>) (clContext: ClContext) workGroupSize =
        let getBitmap =
            Backend.Common.Map.map2<'a, 'b, int> (Map.choose2Bitmap predicate) clContext workGroupSize

        let prefixSum =
            Common.PrefixSum.standardExcludeInPlace clContext workGroupSize

        let assignValues =
            ClArray.assignOption2 predicate clContext workGroupSize

        let scatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (firstValues: ClArray<'a>) (secondValues: ClArray<'b>) (columns: ClArray<int>) (rows: ClArray<int>) ->

            let positions =
                getBitmap processor DeviceOnly firstValues secondValues

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            if resultLength = 0 then
                positions.Free processor

                None
            else
                let resultColumns =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                scatter processor positions columns resultColumns

                let resultRows =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                scatter processor positions rows resultRows

                let resultValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                assignValues processor firstValues secondValues positions resultValues

                positions.Free processor

                Some(resultValues, resultColumns, resultRows)

    let expand (clContext: ClContext) workGroupSize =

        let idScatter =
            Common.Scatter.initLastOccurrence Map.id clContext workGroupSize

        let scatter =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let maxPrefixSum =
            Common.PrefixSum.runIncludeInPlace <@ max @> clContext workGroupSize

        let create = ClArray.create clContext workGroupSize

        let gather =
            Common.Gather.run clContext workGroupSize

        let segmentPrefixSum =
            Common.PrefixSum.ByKey.sequentialInclude <@ (+) @> 0 clContext workGroupSize

        let removeDuplicates =
            ClArray.removeDuplications clContext workGroupSize

        let leftMatrixGather =
            Common.Gather.run clContext workGroupSize

        let rightMatrixGather =
            Common.Gather.run clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (lengths: int) (segmentsPointers: ClArray<int>) (leftMatrix: ClMatrix.COO<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->
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

            let rows =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, lengths)

            gather processor leftMatrixPositions leftMatrix.Rows rows

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
            Common.Sort.Radix.runByKeysStandardValuesOnly clContext workGroupSize

        let sortByKeyValues =
            Common.Sort.Radix.runByKeysStandardValuesOnly clContext workGroupSize

        let sortKeys =
            Common.Sort.Radix.standardRunKeysOnly clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (columns: ClArray<int>) (rows: ClArray<int>) ->
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

    let reduce opAdd (clContext: ClContext) workGroupSize =

        let reduce =
            Common.Reduce.ByKey2D.Option.segmentSequential opAdd clContext workGroupSize

        let getUniqueBitmap =
            Backend.Common.Bitmap.lastOccurrence2 clContext workGroupSize

        let prefixSum =
            Common.PrefixSum.standardExcludeInPlace clContext workGroupSize

        let idScatter =
            Common.Scatter.initFirstOccurrence Map.id clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (values: ClArray<'a>) (columns: ClArray<int>) (rows: ClArray<int>) ->

            let bitmap =
                getUniqueBitmap processor DeviceOnly columns rows

            let uniqueKeysCount =
                (prefixSum processor bitmap)
                    .ToHostAndFree processor

            let offsets =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, uniqueKeysCount)

            idScatter processor bitmap offsets

            bitmap.Free processor

            let reduceResult =
                reduce processor allocationMode uniqueKeysCount offsets columns rows values

            offsets.Free processor

            // reducedValues, reducedColumns, reducedRows option
            reduceResult

    let runCOO opAdd opMul (clContext: ClContext) workGroupSize =

        let getSegmentPointers =
            getSegmentPointers clContext workGroupSize

        let expand = expand clContext workGroupSize

        let multiply = multiply opMul clContext workGroupSize

        let sort =
            sortByColumnsAndRows clContext workGroupSize

        let reduce = reduce opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rightMatrixRowsNNZ: ClArray<int>) (rightMatrix: ClMatrix.CSR<'b>) (leftMatrix: ClMatrix.COO<'a>) ->

            let length, segmentPointers =
                getSegmentPointers processor leftMatrix.Columns rightMatrixRowsNNZ

            if length = 0 then
                segmentPointers.Free processor

                length, None
            else
                // expand
                let leftMatrixValues, rightMatrixValues, columns, rows =
                    expand processor length segmentPointers leftMatrix rightMatrix

                segmentPointers.Free processor

                // multiply
                let mulResult =
                    multiply processor leftMatrixValues rightMatrixValues columns rows

                leftMatrixValues.Free processor
                rightMatrixValues.Free processor
                columns.Free processor
                rows.Free processor

                let result =
                    mulResult
                    |> Option.bind
                        (fun (resultValues, resultColumns, resultRows) ->
                            // sort
                            let sortedValues, sortedColumns, sortedRows =
                                sort processor resultValues resultColumns resultRows

                            resultValues.Free processor
                            resultColumns.Free processor
                            resultRows.Free processor

                            // addition
                            let reduceResult =
                                reduce processor allocationMode sortedValues sortedColumns sortedRows

                            sortedValues.Free processor
                            sortedColumns.Free processor
                            sortedRows.Free processor

                            reduceResult)

                length, result

    let runOneStep opAdd opMul (clContext: ClContext) workGroupSize =

        let runCOO =
            runCOO opAdd opMul clContext workGroupSize

        let expandRowPointers =
            CSR.Matrix.expandRowPointers clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.CSR<'a>) rightMatrixRowsNNZ (rightMatrix: ClMatrix.CSR<'b>) ->

            let rows =
                expandRowPointers processor DeviceOnly leftMatrix

            let leftMatrixCOO =
                { Context = clContext
                  RowCount = leftMatrix.RowCount
                  ColumnCount = leftMatrix.ColumnCount
                  Rows = rows
                  Columns = leftMatrix.Columns
                  Values = leftMatrix.Values }

            let _, result =
                runCOO processor allocationMode rightMatrixRowsNNZ rightMatrix leftMatrixCOO

            rows.Free processor

            result
            |> Option.map
                (fun (values, columns, rows) ->
                    { Context = clContext
                      RowCount = leftMatrix.RowCount
                      ColumnCount = rightMatrix.ColumnCount
                      Rows = rows
                      Columns = columns
                      Values = values })

    let runManySteps opAdd opMul (clContext: ClContext) workGroupSize =

        let gather =
            Common.Gather.run clContext workGroupSize

        let upperBound =
            ClArray.upperBound clContext workGroupSize

        let set = ClArray.set clContext workGroupSize

        let subMatrix =
            CSR.Matrix.subRows clContext workGroupSize

        let runCOO =
            runCOO opAdd opMul clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode maxAllocSize generalLength (leftMatrix: ClMatrix.CSR<'a>) segmentLengths rightMatrixRowsNNZ (rightMatrix: ClMatrix.CSR<'b>) ->
            // extract segment lengths by left matrix rows pointers
            let segmentPointersByLeftMatrixRows =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftMatrix.RowPointers.Length)

            gather processor leftMatrix.RowPointers segmentLengths segmentPointersByLeftMatrixRows

            // set last element to one step length
            set processor segmentPointersByLeftMatrixRows (leftMatrix.RowPointers.Length - 1) generalLength

            // curring
            let upperBound =
                upperBound processor segmentPointersByLeftMatrixRows

            let subMatrix = subMatrix processor DeviceOnly

            let runCOO =
                runCOO processor allocationMode rightMatrixRowsNNZ rightMatrix

            let rec helper beginRow workOffset previousResult =
                if beginRow < leftMatrix.RowCount then
                    let currentBound =
                        clContext.CreateClCell(workOffset + maxAllocSize: int)

                    // find largest row that fit into maxAllocSize
                    let upperBound =
                        (upperBound currentBound).ToHostAndFree processor

                    let endRow = upperBound - 2

                    currentBound.Free processor

                    // TODO(handle largest rows)
                    // (we can split row, multiply and merge them but merge path needed)
                    if endRow = beginRow then
                        failwith "It is impossible to multiply such a long row"

                    // extract matrix TODO(Transfer overhead)
                    let subMatrix =
                        subMatrix beginRow (endRow - beginRow) leftMatrix

                    // compute sub result
                    let length, result = runCOO subMatrix
                    // increase workOffset according to previous expand
                    let workOffset = workOffset + length

                    match result with
                    | Some result ->
                        helper endRow workOffset
                        <| result :: previousResult
                    | None -> helper endRow workOffset previousResult
                else
                    previousResult

            let result = helper 0 0 [] |> List.rev

            segmentPointersByLeftMatrixRows.Free processor

            result

    let run opAdd opMul (clContext: ClContext) workGroupSize =

        let getNNZInRows =
            CSR.Matrix.NNZInRows clContext workGroupSize

        let getSegmentPointers =
            getSegmentPointers clContext workGroupSize

        let runOneStep =
            runOneStep opAdd opMul clContext workGroupSize

        let concat = ClArray.concat clContext workGroupSize

        let concatData = ClArray.concat clContext workGroupSize

        let runManySteps =
            runManySteps opAdd opMul clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode maxAllocSize (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

            let rightMatrixRowsNNZ =
                getNNZInRows processor DeviceOnly rightMatrix

            let generalLength, segmentLengths =
                getSegmentPointers processor leftMatrix.Columns rightMatrixRowsNNZ

            if generalLength = 0 then
                None
            elif generalLength < maxAllocSize then
                segmentLengths.Free processor

                runOneStep processor allocationMode leftMatrix rightMatrixRowsNNZ rightMatrix
            else
                let result =
                    runManySteps
                        processor
                        allocationMode
                        maxAllocSize
                        generalLength
                        leftMatrix
                        segmentLengths
                        rightMatrixRowsNNZ
                        rightMatrix

                rightMatrixRowsNNZ.Free processor
                segmentLengths.Free processor

                match result with
                | _ :: _ ->
                    let valuesList, columnsList, rowsList = result |> List.unzip3

                    let values =
                        concatData processor allocationMode valuesList

                    let columns =
                        concat processor allocationMode columnsList

                    let rows = concat processor allocationMode rowsList

                    // TODO(overhead: compute result length 3 time)
                    // release resources
                    valuesList
                    |> List.iter (fun array -> array.Free processor)

                    columnsList
                    |> List.iter (fun array -> array.Free processor)

                    rowsList
                    |> List.iter (fun array -> array.Free processor)

                    { Context = clContext
                      RowCount = leftMatrix.RowCount
                      ColumnCount = rightMatrix.ColumnCount
                      Rows = rows
                      Columns = columns
                      Values = values }
                    |> Some
                | _ -> None

    module COO =
        let runOneStep opAdd opMul (clContext: ClContext) workGroupSize =

            let runCOO =
                runCOO opAdd opMul clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.COO<'a>) rightMatrixRowsNNZ (rightMatrix: ClMatrix.CSR<'b>) ->

                let _, result =
                    runCOO processor allocationMode rightMatrixRowsNNZ rightMatrix leftMatrix

                result
                |> Option.map
                    (fun (values, columns, rows) ->
                        { Context = clContext
                          RowCount = leftMatrix.RowCount
                          ColumnCount = rightMatrix.ColumnCount
                          Rows = rows
                          Columns = columns
                          Values = values })

        let runManySteps opAdd opMul (clContext: ClContext) workGroupSize =

            let compress =
                COO.Matrix.compressRows clContext workGroupSize

            let gather =
                Common.Gather.run clContext workGroupSize

            let upperBound =
                ClArray.upperBound clContext workGroupSize

            let set = ClArray.set clContext workGroupSize

            let subMatrix =
                COO.Matrix.subRows clContext workGroupSize

            let runCOO =
                runCOO opAdd opMul clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode maxAllocSize generalLength (leftMatrix: ClMatrix.COO<'a>) segmentLengths rightMatrixRowsNNZ (rightMatrix: ClMatrix.CSR<'b>) ->

                let leftRowPointers =
                    compress processor allocationMode leftMatrix.Rows leftMatrix.RowCount

                // extract segment lengths by left matrix rows pointers
                let segmentPointersByLeftMatrixRows =
                    clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftRowPointers.Length)

                gather processor leftRowPointers segmentLengths segmentPointersByLeftMatrixRows

                // set last element to one step length
                set processor segmentPointersByLeftMatrixRows (leftRowPointers.Length - 1) generalLength

                // curring
                let upperBound =
                    upperBound processor segmentPointersByLeftMatrixRows

                let subMatrix = subMatrix processor DeviceOnly

                let runCOO =
                    runCOO processor allocationMode rightMatrixRowsNNZ rightMatrix

                let rec helper beginRow workOffset previousResult =
                    if beginRow < leftMatrix.RowCount then
                        let currentBound =
                            clContext.CreateClCell(workOffset + maxAllocSize: int)

                        // find largest row that fit into maxAllocSize
                        let upperBound =
                            (upperBound currentBound).ToHostAndFree processor

                        let endRow = upperBound - 2

                        currentBound.Free processor

                        // TODO(handle largest rows)
                        // (we can split row, multiply and merge them but merge path needed)
                        if endRow = beginRow then
                            failwith "It is impossible to multiply such a long row"

                        // extract matrix TODO(Transfer overhead)
                        let subMatrix =
                            subMatrix beginRow (endRow - beginRow) leftMatrix

                        // compute sub result
                        let length, result = runCOO subMatrix
                        // increase workOffset according to previous expand
                        let workOffset = workOffset + length

                        match result with
                        | Some result ->
                            helper endRow workOffset
                            <| result :: previousResult
                        | None -> helper endRow workOffset previousResult
                    else
                        previousResult

                let result = helper 0 0 [] |> List.rev

                segmentPointersByLeftMatrixRows.Free processor

                result

        let run opAdd opMul (clContext: ClContext) workGroupSize =

            let getNNZInRows =
                CSR.Matrix.NNZInRows clContext workGroupSize

            let getSegmentPointers =
                getSegmentPointers clContext workGroupSize

            let runOneStep =
                runOneStep opAdd opMul clContext workGroupSize

            let concat = ClArray.concat clContext workGroupSize

            let concatData = ClArray.concat clContext workGroupSize

            let runManySteps =
                runManySteps opAdd opMul clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode maxAllocSize (leftMatrix: ClMatrix.COO<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

                let rightMatrixRowsNNZ =
                    getNNZInRows processor DeviceOnly rightMatrix

                let generalLength, segmentLengths =
                    getSegmentPointers processor leftMatrix.Columns rightMatrixRowsNNZ

                if generalLength = 0 then
                    None
                elif generalLength < maxAllocSize then
                    segmentLengths.Free processor

                    runOneStep processor allocationMode leftMatrix rightMatrixRowsNNZ rightMatrix
                else
                    let result =
                        runManySteps
                            processor
                            allocationMode
                            maxAllocSize
                            generalLength
                            leftMatrix
                            segmentLengths
                            rightMatrixRowsNNZ
                            rightMatrix

                    rightMatrixRowsNNZ.Free processor
                    segmentLengths.Free processor

                    match result with
                    | _ :: _ ->
                        let valuesList, columnsList, rowsList = result |> List.unzip3

                        let values =
                            concatData processor allocationMode valuesList

                        let columns =
                            concat processor allocationMode columnsList

                        let rows = concat processor allocationMode rowsList

                        // TODO(overhead: compute result length 3 time)
                        // release resources
                        valuesList
                        |> List.iter (fun array -> array.Free processor)

                        columnsList
                        |> List.iter (fun array -> array.Free processor)

                        rowsList
                        |> List.iter (fun array -> array.Free processor)

                        { Context = clContext
                          RowCount = leftMatrix.RowCount
                          ColumnCount = rightMatrix.ColumnCount
                          Rows = rows
                          Columns = columns
                          Values = values }
                        |> Some
                    | _ -> None
