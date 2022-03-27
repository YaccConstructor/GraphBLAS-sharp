
open GraphBLAS.FSharp.Backend
open Brahma.FSharp.OpenCL
open System.IO
// open GraphBLAS.FSharp.Backend.Common

// open GraphBLAS.FSharp

// open Brahma.FSharp.OpenCL
// open OpenCL.Net
// open GraphBLAS.FSharp.Predefined
// open Microsoft.FSharp.Quotations

// open System.IO
// open Brahma.FSharp.OpenCL
// open Brahma.FSharp.OpenCL.Translator
// open Brahma.FSharp.OpenCL.Printer.AST
// open FSharp.Quotations

[<EntryPoint>]
let main argv =
    let clContext = ClContext()
    let processor = clContext.CommandQueue

    let n = 100
    let workGroupSize = 128
    let path = "test.mtx"

    let makeCOO (path: string) : GraphBLAS.FSharp.Backend.COOMatrix<float> =
        use streamReader = new StreamReader(path)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let size =
            streamReader.ReadLine().Split(' ')
            |> Array.map int

        let len = size.[2]

        let data =
            [0 .. len - 1]
            |> List.map (fun _ -> streamReader.ReadLine().Split(' '))

        let pack x y = ((uint64 x) <<< 32) ||| (uint64 y)
        let unpack x = (int (x >>> 32)), (int x)

        data
        |> Array.ofList
        |> Array.Parallel.map
            (fun line ->
                //let value = Convert.ChangeType(line.[2], typeof<'a>) |> unbox<'a>
                struct(pack <| int line.[0] <| int line.[1], 1.0)
            )
        |> Array.sortBy (fun struct(packedIndex, _) -> packedIndex)
        |>
            fun data ->
                let rows = Array.zeroCreate data.Length
                let cols = Array.zeroCreate data.Length
                let values = Array.zeroCreate data.Length

                Array.Parallel.iteri (fun i struct(packedIndex, value) ->
                    let (rowIdx, columnIdx) = unpack packedIndex
                    // in mtx indecies start at 1
                    rows.[i] <- rowIdx - 1
                    cols.[i] <- columnIdx - 1
                    values.[i] <- value
                ) data

                let rows = clContext.CreateClArray(rows)
                let cols = clContext.CreateClArray(cols)
                let values = clContext.CreateClArray(values)

                {
                    Context = clContext
                    Rows = rows
                    Columns = cols
                    Values = values
                    RowCount = size.[0]
                    ColumnCount = size.[1]
                }

    let getUniqueBitmap (clContext: ClContext) =
        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isUniqueBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputLength - 1
                   && inputArray.[i] = inputArray.[i + 1] then
                    isUniqueBitmap.[i] <- 0
                else
                    isUniqueBitmap.[i] <- 1 @>
        let getUniqueBitmap = clContext.CreateClProgram(getUniqueBitmap).GetKernel()
        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) ->
            let inputLength = inputArray.Length
            let ndRange =
                Range1D.CreateValid(inputLength, workGroupSize)
            let bitmap =
                clContext.CreateClArray(
                    inputLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )
            processor.Post(
                Msg.MsgSetArguments(fun () -> getUniqueBitmap.KernelFunc ndRange inputArray inputLength bitmap)
            )
            processor.Post(Msg.CreateRunMsg<_, _> getUniqueBitmap)
            bitmap

    let compressRows (clContext: ClContext) workGroupSize =
        let calcHyperSparseRows =
            <@ fun (ndRange: Range1D) (rowsIndices: ClArray<int>) (bitmap: ClArray<int>) (positions: ClArray<int>) (nonZeroRowsIndices: ClArray<int>) (nonZeroRowsPointers: ClArray<int>) nnz ->

                let gid = ndRange.GlobalID0

                if gid < nnz && bitmap.[gid] = 1 then
                    nonZeroRowsIndices.[positions.[gid]] <- rowsIndices.[gid]
                    nonZeroRowsPointers.[positions.[gid]] <- gid + 1 @>
        let calcNnzPerRowSparse =
            <@ fun (ndRange: Range1D) (nonZeroRowsPointers: ClArray<int>) (nnzPerRowSparse: ClArray<int>) totalSum ->
                let gid = ndRange.GlobalID0
                if gid = 0 then
                    nnzPerRowSparse.[gid] <- nonZeroRowsPointers.[gid]
                elif gid < totalSum then
                    nnzPerRowSparse.[gid] <-
                        nonZeroRowsPointers.[gid]
                        - nonZeroRowsPointers.[gid - 1] @>
        let expandNnzPerRow =
            <@ fun (ndRange: Range1D) totalSum (nnzPerRowSparse: ClArray<'a>) (nonZeroRowsIndices: ClArray<int>) (expandedNnzPerRow: ClArray<'a>) ->
                let i = ndRange.GlobalID0
                if i < totalSum then
                    expandedNnzPerRow.[nonZeroRowsIndices.[i] + 1] <- nnzPerRowSparse.[i] @>
        let kernelCalcHyperSparseRows =
            clContext.CreateClProgram(calcHyperSparseRows).GetKernel()
            // clContext.CreateClKernel calcHyperSparseRows
        let kernelCalcNnzPerRowSparse =
            clContext.CreateClProgram(calcNnzPerRowSparse).GetKernel()
            // clContext.CreateClKernel calcNnzPerRowSparse
        let kernelExpandNnzPerRow =
            clContext.CreateClProgram(expandNnzPerRow).GetKernel()
            // clContext.CreateClKernel expandNnzPerRow
        let getUniqueBitmap = getUniqueBitmap clContext
        let posAndTotalSum =
            ClArray.prefixSumExclude clContext workGroupSize
        let getRowPointers =
            ClArray.prefixSumInclude clContext workGroupSize
        fun (processor: MailboxProcessor<_>) (rowIndices: ClArray<int>) rowCount ->
            let bitmap =
                getUniqueBitmap processor workGroupSize rowIndices
            let positions, totalSum = posAndTotalSum processor bitmap
            let hostTotalSum = [| 0 |]
            let _ =
                processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(totalSum, hostTotalSum, ch))
            let totalSum = hostTotalSum.[0]
            let nonZeroRowsIndices = clContext.CreateClArray totalSum
            let nonZeroRowsPointers = clContext.CreateClArray totalSum
            let nnz = rowIndices.Length
            let ndRangeCHSR = Range1D.CreateValid(nnz, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernelCalcHyperSparseRows.KernelFunc
                            ndRangeCHSR
                            rowIndices
                            bitmap
                            positions
                            nonZeroRowsIndices
                            nonZeroRowsPointers
                            nnz)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernelCalcHyperSparseRows)
            let nnzPerRowSparse = clContext.CreateClArray totalSum
            let ndRangeCNPRSandENPR =
                Range1D.CreateValid(totalSum, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernelCalcNnzPerRowSparse.KernelFunc
                            ndRangeCNPRSandENPR
                            nonZeroRowsPointers
                            nnzPerRowSparse
                            totalSum)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernelCalcNnzPerRowSparse)
            let expandedNnzPerRow =
                clContext.CreateClArray(Array.zeroCreate rowCount)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernelExpandNnzPerRow.KernelFunc
                            ndRangeCNPRSandENPR
                            totalSum
                            nnzPerRowSparse
                            nonZeroRowsIndices
                            expandedNnzPerRow)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernelExpandNnzPerRow)
            let rowPointers, _ =
                getRowPointers processor expandedNnzPerRow
            rowPointers

    let toCSR clContext workGroupSize =
        let compressRows = compressRows clContext workGroupSize
        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize
        let copyData =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize
        fun (processor: MailboxProcessor<_>) (matrix: GraphBLAS.FSharp.Backend.COOMatrix<float>) ->
            let compressedRows =
                compressRows processor matrix.Rows matrix.RowCount
            let cols =
                copy processor matrix.Columns
            let vals =
                copyData processor matrix.Values
            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = compressedRows
              Columns = cols
              Values = vals }


    let m = makeCOO path
    // let fmtx = toCSR clContext workGroupSize processor m
    // let smtx = toCSR clContext workGroupSize processor m

    let frPCPU = Array.zeroCreate m.Rows.Length
    let fcCPU = Array.zeroCreate m.Columns.Length
    let fvCPU = Array.zeroCreate m.Values.Length

    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(m.Rows, frPCPU, ch))
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(m.Columns, fcCPU, ch))
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(m.Values, fvCPU, ch))

    printfn "%A" m.RowCount
    printfn "%A" m.ColumnCount
    printfn "%A" frPCPU
    printfn "%A" fcCPU
    printfn "%A" fvCPU

    let gg = COOMatrix.eWiseAdd clContext <@ (+) @> workGroupSize processor m m

    let frPCPU = Array.zeroCreate gg.Rows.Length
    let fcCPU = Array.zeroCreate gg.Columns.Length
    let fvCPU = Array.zeroCreate gg.Values.Length

    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gg.Rows, frPCPU, ch))
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gg.Columns, fcCPU, ch))
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gg.Values, fvCPU, ch))

    printfn "%A" gg.RowCount
    printfn "%A" gg.ColumnCount
    printfn "%A" frPCPU
    printfn "%A" fcCPU
    printfn "%A" fvCPU

    // let spgemm = CSRMatrix.spgemm <@ (*) @> <@ (+) @> 0.0 clContext workGroupSize processor
    // // let rmtx = CSRMatrix.spgemm <@ (*) @> <@ (+) @> 0.0 clContext workGroupSize processor fmtx smtx
    // for i in 0 .. 1000 do
    //     let rmtx = spgemm fmtx smtx
    //     ()

    0
