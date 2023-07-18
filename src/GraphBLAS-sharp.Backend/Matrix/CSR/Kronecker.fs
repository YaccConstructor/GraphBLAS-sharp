namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open FSharpx.Collections
open Microsoft.FSharp.Quotations
open FSharp.Quotations.Evaluator.QuotationEvaluationExtensions
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects.ClCellExtensions
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module internal Kronecker =
    let private updateBitmap (clContext: ClContext) workGroupSize op =

        let updateBitmap (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) (operand: ClCell<'a>) valuesLength zeroCount (values: ClArray<'b>) (resultBitmap: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid = 0 then

                    let item = resultBitmap.[0]
                    let newItem = item + zeroCount

                    match (%op) (Some operand.Value) None with
                    | Some _ -> resultBitmap.[0] <- newItem
                    | _ -> ()

                elif (gid - 1) < valuesLength then

                    let item = resultBitmap.[gid]
                    let newItem = item + 1

                    match (%op) (Some operand.Value) (Some values.[gid - 1]) with
                    | Some _ -> resultBitmap.[gid] <- newItem
                    | _ -> () @>

        let updateBitmap = clContext.Compile <| updateBitmap op

        fun (processor: MailboxProcessor<_>) (operand: ClCell<'a>) (matrixRight: CSR<'b>) (bitmap: ClArray<int>) ->

            let resultLength = matrixRight.NNZ + 1

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            let updateBitmap = updateBitmap.GetKernel()

            let numberOfZeros =
                matrixRight.ColumnCount * matrixRight.RowCount
                - matrixRight.NNZ

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        updateBitmap.KernelFunc ndRange operand matrixRight.NNZ numberOfZeros matrixRight.Values bitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _> updateBitmap)

    let private getAllocationSize (clContext: ClContext) workGroupSize op =

        let updateBitmap = updateBitmap clContext workGroupSize op

        let sum =
            Reduce.sum <@ fun x y -> x + y @> 0 clContext workGroupSize

        let item = ClArray.item clContext workGroupSize

        let createClArray =
            ClArray.zeroCreate clContext workGroupSize

        let opOnHost = op.Evaluate()

        fun (queue: MailboxProcessor<_>) (matrixZero: COO<'c> option) (matrixLeft: CSR<'a>) (matrixRight: CSR<'b>) ->

            let nnz =
                match opOnHost None None with
                | Some _ ->
                    let leftZeroCount =
                        matrixLeft.RowCount * matrixLeft.ColumnCount
                        - matrixLeft.NNZ

                    let rightZeroCount =
                        matrixRight.RowCount * matrixRight.ColumnCount
                        - matrixRight.NNZ

                    leftZeroCount * rightZeroCount
                | _ -> 0

            let bitmap =
                createClArray queue DeviceOnly (matrixRight.NNZ + 1)

            for index in 0 .. matrixLeft.NNZ - 1 do
                let value = item queue index matrixLeft.Values

                updateBitmap queue value matrixRight bitmap

                value.Free queue

            let bitmapSum = sum queue bitmap

            bitmap.Free queue

            let leftZeroCount =
                matrixLeft.ColumnCount * matrixLeft.RowCount
                - matrixLeft.NNZ

            match matrixZero with
            | Some m -> m.NNZ * leftZeroCount
            | _ -> 0
            + nnz
            + bitmapSum.ToHostAndFree queue

    let private preparePositions<'a, 'b, 'c when 'b: struct> (clContext: ClContext) workGroupSize op =

        let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) (operand: ClCell<'a>) rowCount columnCount (values: ClArray<'b>) (rowPointers: ClArray<int>) (columns: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < rowCount * columnCount then

                    let columnIndex = gid % columnCount
                    let rowIndex = gid / columnCount

                    let firstIndex = rowPointers.[rowIndex]
                    let lastIndex = rowPointers.[rowIndex + 1] - 1

                    let value =
                        (%Search.Bin.inRange) firstIndex lastIndex columnIndex columns values

                    match (%op) (Some operand.Value) value with
                    | Some resultValue ->
                        resultValues.[gid] <- resultValue
                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel = clContext.Compile <| preparePositions op

        fun (processor: MailboxProcessor<_>) (operand: ClCell<'a>) (matrix: CSR<'b>) (resultDenseMatrix: ClArray<'c>) (resultBitmap: ClArray<int>) ->

            let resultLength = matrix.RowCount * matrix.ColumnCount

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            operand
                            matrix.RowCount
                            matrix.ColumnCount
                            matrix.Values
                            matrix.RowPointers
                            matrix.Columns
                            resultBitmap
                            resultDenseMatrix)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private setPositions<'c when 'c: struct> (clContext: ClContext) workGroupSize =

        let setPositions =
            <@ fun (ndRange: Range1D) rowCount columnCount startIndex (rowOffset: ClCell<int>) (columnOffset: ClCell<int>) (bitmap: ClArray<int>) (values: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) (resultValues: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < rowCount * columnCount
                   && (gid = 0 && bitmap.[gid] = 1
                       || gid > 0 && bitmap.[gid - 1] < bitmap.[gid]) then

                    let columnIndex = gid % columnCount
                    let rowIndex = gid / columnCount

                    let index = startIndex + bitmap.[gid] - 1

                    resultRows.[index] <- rowIndex + rowOffset.Value
                    resultColumns.[index] <- columnIndex + columnOffset.Value
                    resultValues.[index] <- values.[gid] @>

        let kernel = clContext.Compile <| setPositions

        let scan =
            PrefixSum.standardIncludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) rowCount columnCount (rowOffset: int) (columnOffset: int) (startIndex: int) (resultMatrix: COO<'c>) (values: ClArray<'c>) (bitmap: ClArray<int>) ->

            let sum = scan processor bitmap

            let ndRange =
                Range1D.CreateValid(rowCount * columnCount, workGroupSize)

            let kernel = kernel.GetKernel()

            let rowOffset = rowOffset |> clContext.CreateClCell
            let columnOffset = columnOffset |> clContext.CreateClCell

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            rowCount
                            columnCount
                            startIndex
                            rowOffset
                            columnOffset
                            bitmap
                            values
                            resultMatrix.Rows
                            resultMatrix.Columns
                            resultMatrix.Values)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            (sum.ToHostAndFree processor) + startIndex

    let private copyToResult (clContext: ClContext) workGroupSize =

        let copyToResult =
            <@ fun (ndRange: Range1D) startIndex sourceLength (rowOffset: ClCell<int>) (columnOffset: ClCell<int>) (sourceRows: ClArray<int>) (sourceColumns: ClArray<int>) (sourceValues: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) (resultValues: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < sourceLength then
                    let index = startIndex + gid

                    resultRows.[index] <- rowOffset.Value + sourceRows.[gid]
                    resultColumns.[index] <- columnOffset.Value + sourceColumns.[gid]
                    resultValues.[index] <- sourceValues.[gid] @>

        let kernel = clContext.Compile <| copyToResult

        fun (processor: MailboxProcessor<_>) startIndex (rowOffset: int) (columnOffset: int) (resultMatrix: COO<'c>) (sourceMatrix: COO<'c>) ->

            let ndRange =
                Range1D.CreateValid(sourceMatrix.NNZ, workGroupSize)

            let kernel = kernel.GetKernel()

            let rowOffset = rowOffset |> clContext.CreateClCell
            let columnOffset = columnOffset |> clContext.CreateClCell

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            startIndex
                            sourceMatrix.NNZ
                            rowOffset
                            columnOffset
                            sourceMatrix.Rows
                            sourceMatrix.Columns
                            sourceMatrix.Values
                            resultMatrix.Rows
                            resultMatrix.Columns
                            resultMatrix.Values)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private insertZero (clContext: ClContext) workGroupSize =

        let copy = copyToResult clContext workGroupSize

        fun queue startIndex (zeroCounts: int list array) (matrixZero: COO<'c>) resultMatrix ->

            let rowCount = zeroCounts.Length

            let mutable startIndex = startIndex

            let insertMany row firstColumn count =
                for i in 0 .. count - 1 do
                    let rowOffset = row * matrixZero.RowCount

                    let columnOffset =
                        (firstColumn + i) * matrixZero.ColumnCount

                    copy queue startIndex rowOffset columnOffset resultMatrix matrixZero

                    startIndex <- startIndex + matrixZero.NNZ

            let rec insertInRowRec zeroCounts row column =
                match zeroCounts with
                | [] -> ()
                | h :: tl ->
                    insertMany row column h

                    insertInRowRec tl row (h + column + 1)

            for row in 0 .. rowCount - 1 do
                insertInRowRec zeroCounts.[row] row 0

    let private insertNonZero (clContext: ClContext) workGroupSize op =

        let item = ClArray.item clContext workGroupSize

        let preparePositions =
            preparePositions clContext workGroupSize op

        let setPositions = setPositions clContext workGroupSize

        fun queue (rowsEdges: (int * int) array) (matrixRight: CSR<'b>) (leftValues: ClArray<'a>) (leftColsHost: int array) (resultMatrix: COO<'c>) ->

            let setPositions =
                setPositions queue matrixRight.RowCount matrixRight.ColumnCount

            let rowCount = rowsEdges.Length

            let length =
                matrixRight.RowCount * matrixRight.ColumnCount

            let bitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, length)

            let mappedMatrix =
                clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, length)

            let mutable startIndex = 0

            for row in 0 .. rowCount - 1 do
                let leftEdge, rightEdge = rowsEdges.[row]

                for i in leftEdge .. rightEdge do
                    let value = item queue i leftValues
                    let column = leftColsHost.[i]

                    let rowOffset = row * matrixRight.RowCount
                    let columnOffset = column * matrixRight.ColumnCount

                    preparePositions queue value matrixRight mappedMatrix bitmap

                    value.Free queue

                    startIndex <- setPositions rowOffset columnOffset startIndex resultMatrix mappedMatrix bitmap

            bitmap.Free queue
            mappedMatrix.Free queue

            startIndex

    let private mapAll<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        workGroupSize
        (op: Expr<'a option -> 'b option -> 'c option>)
        =

        let insertNonZero = insertNonZero clContext workGroupSize op

        let insertZero = insertZero clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (resultNNZ: int) (matrixZero: COO<'c> option) (matrixLeft: CSR<'a>) (matrixRight: CSR<'b>) ->

            let resultRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultNNZ)

            let resultColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultNNZ)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'c>(allocationMode, resultNNZ)

            let resultMatrix =
                { Context = clContext
                  Rows = resultRows
                  Columns = resultColumns
                  Values = resultValues
                  RowCount = matrixLeft.RowCount * matrixRight.RowCount
                  ColumnCount = matrixLeft.ColumnCount * matrixRight.ColumnCount }

            let leftRowPointers = matrixLeft.RowPointers.ToHost queue
            let leftColumns = matrixLeft.Columns.ToHost queue

            let nnzInRows =
                leftRowPointers
                |> Array.pairwise
                |> Array.map (fun (fst, snd) -> snd - fst)

            let rowsEdges =
                leftRowPointers
                |> Array.pairwise
                |> Array.map (fun (fst, snd) -> (fst, snd - 1))

            let (zeroCounts: int list array) = Array.zeroCreate matrixLeft.RowCount

            { 0 .. matrixLeft.RowCount - 1 }
            |> Seq.iter2
                (fun edges i ->
                    zeroCounts.[i] <-
                        leftColumns.[fst edges..snd edges]
                        |> Array.toList
                        |> List.insertAt 0 -1
                        |> List.insertAt (nnzInRows.[i] + 1) matrixLeft.ColumnCount
                        |> List.pairwise
                        |> List.map (fun (fstCol, sndCol) -> sndCol - fstCol - 1))
                rowsEdges

            let startIndex =
                insertNonZero queue rowsEdges matrixRight matrixLeft.Values leftColumns resultMatrix

            matrixZero
            |> Option.iter (fun m -> insertZero queue startIndex zeroCounts m resultMatrix)

            resultMatrix

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        workGroupSize
        (op: Expr<'a option -> 'b option -> 'c option>)
        =

        let getSize =
            getAllocationSize clContext workGroupSize op

        let mapWithValue =
            Map.WithValue.run clContext op workGroupSize

        let mapAll = mapAll clContext workGroupSize op

        let bitonic =
            Sort.Bitonic.sortKeyValuesInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: CSR<'a>) (matrixRight: CSR<'b>) ->

            let matrixZero =
                mapWithValue queue allocationMode None matrixRight

            let size =
                getSize queue matrixZero matrixLeft matrixRight

            if size = 0 then
                matrixZero
                |> Option.iter (fun m -> m.Dispose queue)

                None
            else
                let result =
                    mapAll queue allocationMode size matrixZero matrixLeft matrixRight

                matrixZero
                |> Option.iter (fun m -> m.Dispose queue)

                bitonic queue result.Rows result.Columns result.Values

                result |> Some
