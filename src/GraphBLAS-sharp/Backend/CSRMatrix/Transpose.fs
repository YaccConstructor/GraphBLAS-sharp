namespace GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open Brahma.OpenCL

module rec Transpose =
    let t (matrix: CSRMatrix<'a>) = opencl {
        let n = matrix.RowCount
        let! coo = csr2coo matrix
        let! c = Copy.copyArray coo.Columns
        let! r = Copy.copyArray coo.Rows
        let! vs = Copy.copyArray coo.Values

        let! _ = ToHost c
        let! _ = ToHost r
        let! _ = ToHost vs
        printfn "%A" c
        printfn "%A" r
        printfn "%A" vs


        let! ind = pack c r

        do! BitonicSort.sortInplace2 ind vs
        let! (row, col) = unpack ind
        let! _ = ToHost row
        let! _ = ToHost col
        let! _ = ToHost vs
        printfn "%A" row
        printfn "%A" col
        printfn "%A" vs

        let! cr = compressRows matrix.ColumnCount row
        let! _ = ToHost cr
        printfn "%A" cr
        return {
            RowCount = matrix.ColumnCount
            ColumnCount = n
            RowPointers = cr
            ColumnIndices = col
            Values = vs
        }
        // return matrix
    }

    let pack (a: int[]) (b: int[]) = opencl {
        let n = a.Length

        let pack = <@ fun x y -> (uint64 x <<< 32) ||| (uint64 y) @>

        let kernel =
            <@
                fun (range: _1D)
                    (a: int[])
                    (b: int[])
                    (c: uint64[]) ->

                    let gid = range.GlobalID0
                    if gid < n then
                        c.[gid] <- (%pack) a.[gid] b.[gid]
            @>

        let c = Array.zeroCreate<uint64> n

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize n, Utils.defaultWorkGroupSize)
            <| a
            <| b
            <| c

        return c
    }

    let unpack (c: uint64[]) = opencl {
        let n = c.Length

        // let unpackFst = <@ fun x -> int ((x &&& 0xFFFFFFFF0000000UL) >>> 32) @>
        // let unpackSnd = <@ fun x -> int (x &&& 0xFFFFFFFUL) @>

        let kernel =
            <@
                fun (range: _1D)
                    (a: int[])
                    (b: int[])
                    (c: uint64[]) ->

                    let gid = range.GlobalID0
                    if gid < n then
                        a.[gid] <- int ((c.[gid] &&& 0xFFFFFFFF0000000UL) >>> 32)
                        b.[gid] <- int (c.[gid] &&& 0xFFFFFFFUL)
            @>

        let a = Array.zeroCreate<int> n
        let b = Array.zeroCreate<int> n

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize n, Utils.defaultWorkGroupSize)
            <| a
            <| b
            <| c

        return a, b
    }

    let csr2coo (matrix: CSRMatrix<'a>) = opencl {
        let wgSize = Utils.defaultWorkGroupSize
        let expandRows =
            <@
                fun (range: _1D)
                    (rowPointers: int[])
                    (rowIndices: int[]) ->

                    let lid = range.LocalID0
                    let groupId = range.GlobalID0 / wgSize

                    let rowStart = rowPointers.[groupId]
                    let rowEnd = rowPointers.[groupId + 1]
                    let rowLength = rowEnd - rowStart

                    let mutable i = lid
                    while i < rowLength do
                        rowIndices.[rowStart + i] <- groupId
                        i <- i + wgSize
            @>

        let rowIndices = Array.zeroCreate<int> matrix.Values.Length
        do! RunCommand expandRows <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(wgSize * matrix.RowCount, wgSize)
            <| matrix.RowPointers
            <| rowIndices

        let! colIndices = Copy.copyArray matrix.ColumnIndices
        let! values = Copy.copyArray matrix.Values

        return {
            RowCount = matrix.RowCount
            ColumnCount = matrix.ColumnCount
            Rows = rowIndices
            Columns = colIndices
            Values = values
        }
    }

    let compressRows m (rowIndices: int[]) = opencl {
        let n = rowIndices.Length

        let getUniqueBitmap =
            <@
                fun (ndRange: _1D)
                    (inputArray: int[])
                    (isUniqueBitmap: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < n - 1 && inputArray.[i] = inputArray.[i + 1] then
                        isUniqueBitmap.[i] <-    0
            @>

        let bitmap = Array.create n 1
        do! RunCommand getUniqueBitmap <| fun kernelPrepare ->
            let range = _1D(Utils.getDefaultGlobalSize n, Utils.defaultWorkGroupSize)
            kernelPrepare range rowIndices bitmap

        let! (pos, ts) = PrefixSum.runExclude bitmap
        let! _ = ToHost ts

        let a = Array.zeroCreate ts.[0]
        let b = Array.zeroCreate ts.[0]
        let ts = ts.[0]

        let kern =
            <@
                fun (ndRange: _1D)
                    (bitmap: int[])
                    (array: int[])
                    (pos: int[])
                    (row: int[])
                    (off: int[]) ->

                    let gid = ndRange.GlobalID0

                    if gid < n && bitmap.[gid] = 1 then
                        row.[pos.[gid]] <- array.[gid]
                        off.[pos.[gid]] <- gid + 1
            @>

        do! RunCommand kern <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize n, Utils.defaultWorkGroupSize)
            <| bitmap
            <| rowIndices
            <| pos
            <| a
            <| b

        let! _ = ToHost pos
        let! _ = ToHost a
        let! _ = ToHost b

        printfn "%A" pos
        printfn "row = %A" a
        printfn "off = %A" b

        let kern3 =
            <@
                fun (ndRange: _1D)
                    (off: int[])
                    (off3: int[]) ->

                    let gid = ndRange.GlobalID0
                    if gid = 0 then
                        off3.[gid] <- off.[gid]
                    elif gid < ts then
                        off3.[gid] <- off.[gid] - off.[gid - 1]
            @>

        let off3 = Array.zeroCreate ts

        do! RunCommand kern3 <| fun kp ->
            kp
            <| _1D(Utils.getDefaultGlobalSize ts, Utils.defaultWorkGroupSize)
            <| b
            <| off3

        let! _ = ToHost off3
        printfn "off3 = %A" off3

        let kern2 =
            <@
                fun (ndRange: _1D)
                    (off3: int[])
                    (row: int[])
                    (off2: int[]) ->

                    let gid = ndRange.GlobalID0

                    if gid < ts then
                        off2.[row.[gid] + 1] <- off3.[gid]
            @>

        let off2 = Array.zeroCreate (m + 1)

        do! RunCommand kern2 <| fun kp ->
            kp
            <| _1D(Utils.getDefaultGlobalSize ts, Utils.defaultWorkGroupSize)
            <| off3
            <| a
            <| off2

        let! _ = ToHost off2
        printfn "%A" off2

        let! (rp, _) = PrefixSum.runInclude off2
        return rp
    }
