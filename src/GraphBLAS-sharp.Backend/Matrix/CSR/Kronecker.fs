namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open FSharp.Quotations.Evaluator
open FSharpx.Collections
open Microsoft.FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module internal Kronecker =
    let private getBitmap (clContext: ClContext) workGroupSize op =

        let getBitmap (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) (prevSum: ClCell<int>) (operand: ClCell<'a>) valuesLength numberOfZeros (values: ClArray<'b>) (resultBitmap: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid = 0 then

                    match (%op) (Some operand.Value) None with
                    | Some _ -> resultBitmap.[0] <- prevSum.Value + numberOfZeros
                    | _ -> resultBitmap.[0] <- prevSum.Value

                else if (gid - 1) < valuesLength then

                    match (%op) (Some operand.Value) (Some values.[gid - 1]) with
                    | Some _ -> resultBitmap.[gid] <- 1
                    | _ -> resultBitmap.[gid] <- 0 @>

        let getBitmap = clContext.Compile <| getBitmap op

        fun (processor: MailboxProcessor<_>) (prevSum: ClCell<int>) (operand: ClCell<'a>) (matrixRight: ClMatrix.CSR<'b>) (bitmap: ClArray<int>) ->

            let resultLength = matrixRight.NNZ + 1

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            let getBitmap = getBitmap.GetKernel()

            let numberOfZeros =
                matrixRight.ColumnCount * matrixRight.RowCount - matrixRight.NNZ

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        getBitmap.KernelFunc
                            ndRange
                            prevSum
                            operand
                            matrixRight.NNZ
                            numberOfZeros
                            matrixRight.Values
                            bitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _> getBitmap)

    let private getAllocationSize (clContext: ClContext) workGroupSize op =

        let getBitmap = getBitmap clContext workGroupSize op

        let sum =
            Reduce.sum <@ fun x y -> x + y @> 0 clContext workGroupSize

        let item = ClArray.item clContext workGroupSize

        let opOnHost = QuotationEvaluator.Evaluate op

        fun (queue: MailboxProcessor<_>) (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->

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
                |> clContext.CreateClCell

            let bitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, matrixRight.NNZ + 1)

            let nnz =
                { 0 .. matrixLeft.NNZ - 1 }
                |> Seq.fold
                    (fun acc index ->
                        let value = item queue index matrixLeft.Values

                        getBitmap queue acc value matrixRight bitmap

                        let nnz = sum queue bitmap

                        acc.Free queue
                        value.Free queue

                        nnz)
                    nnz

            nnz.ToHostAndFree queue

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

        fun (processor: MailboxProcessor<_>) (operand: ClCell<'a>) (matrix: ClMatrix.CSR<'b>) (resultDenseMatrix: ClArray<'c>) (resultBitmap: ClArray<int>) ->

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

    let setPositions<'c when 'c: struct> (clContext: ClContext) workGroupSize =

        let setPositions =
            <@ fun (ndRange: Range1D) rowCount columnCount startIndex (nnz: ClCell<int>) (rowOffset: ClCell<int>) (columnOffset: ClCell<int>) (bitmap: ClArray<int>) (values: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) (resultValues: ClArray<'c>) ->

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
                            sum
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

    let copyToResult (clContext: ClContext) workGroupSize =

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

            let ndRange = Range1D.CreateValid(sourceMatrix.NNZ, workGroupSize)

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

    let insertZero (clContext: ClContext) workGroupSize =

        let copy = copyToResult clContext workGroupSize

        fun queue startIndex (zeroCounts: int list array) (matrixZero: COO<'c>) resultMatrix ->

            let rowCount = zeroCounts.Length

            let mutable startIndex = startIndex

            let insertMany row firstColumn count =
                let rec insertManyRec iter =
                    if iter >= count then
                        ()
                    else
                        let rowOffset = row * matrixZero.RowCount

                        let columnOffset =
                            (firstColumn + iter) * matrixZero.ColumnCount

                        copy queue startIndex rowOffset columnOffset resultMatrix matrixZero

                        startIndex <- startIndex + matrixZero.NNZ

                        insertManyRec (iter + 1)

                insertManyRec 0

            let rec insertInRowRec zeroCounts row column =
                match zeroCounts with
                | [] -> ()
                | h :: tl ->
                    insertMany row column h

                    insertInRowRec tl row (h + column + 1)

            let rec insertZeroRec row =
                if row >= rowCount then
                    ()
                else
                    insertInRowRec zeroCounts.[row] row 0

                    insertZeroRec (row + 1)

            insertZeroRec 0

    let insertNonZero (clContext: ClContext) workGroupSize op =

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

            let rec insertInRowRec row rightEdge index =
                if index > rightEdge then
                    ()
                else
                    let value = item queue index leftValues
                    let column = leftColsHost.[index]

                    let rowOffset = row * matrixRight.RowCount
                    let columnOffset = column * matrixRight.ColumnCount

                    preparePositions queue value matrixRight mappedMatrix bitmap

                    value.Free queue

                    startIndex <-
                        setPositions rowOffset columnOffset startIndex resultMatrix mappedMatrix bitmap
                    // printfn $"resultMatrix.Values: %A{resultMatrix.Values.ToHost queue}"
                    // printfn $"resultMatrix.Rows: %A{resultMatrix.Rows.ToHost queue}"
                    // printfn $"resultMatrix.Columns: %A{resultMatrix.Columns.ToHost queue}"
                    // printfn $"startIndex: %A{startIndex.ToHost queue}"

                    insertInRowRec row rightEdge (index + 1)

            let rec insertNonZeroRec row =
                if row >= rowCount then
                    ()
                else
                    let leftEdge, rightEdge = rowsEdges.[row]

                    insertInRowRec row rightEdge leftEdge

                    insertNonZeroRec (row + 1)

            insertNonZeroRec 0

            bitmap.Free queue
            mappedMatrix.Free queue

            startIndex

    let mapAll<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
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

            match matrixZero with
            | Some m ->
                insertZero queue startIndex zeroCounts m resultMatrix
            | _ -> ()

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

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->

            let matrixZero =
                mapWithValue queue allocationMode None matrixRight

            let size = getSize queue matrixLeft matrixRight

            let leftZeroCount =
                matrixLeft.ColumnCount * matrixLeft.RowCount
                - matrixLeft.NNZ

            let size =
                match matrixZero with
                | Some m -> size + m.NNZ * leftZeroCount
                | _ -> size

            if size = 0 then
                None
            else
                let result =
                    mapAll queue allocationMode size matrixZero matrixLeft matrixRight

                match matrixZero with
                | Some m -> m.Dispose queue
                | _ -> ()

                bitonic queue result.Rows result.Columns result.Values

                result |> Some
