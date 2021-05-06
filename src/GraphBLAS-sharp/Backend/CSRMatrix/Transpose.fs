namespace GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open Brahma.OpenCL

module internal rec Transpose =
    let transposeMatrix (matrix: CSRMatrix<'a>) = opencl {
        let! coo = csr2coo matrix
        let! packedIndices = pack coo.Columns coo.Rows

        do! BitonicSort.sortKeyValuesInplace packedIndices coo.Values
        let! (rows, cols) = unpack packedIndices

        let! compressedRows = compressRows matrix.ColumnCount rows

        return {
            RowCount = matrix.ColumnCount
            ColumnCount = matrix.ColumnCount
            RowPointers = compressedRows
            ColumnIndices = cols
            Values = coo.Values
        }
    }

    let private csr2coo (matrix: CSRMatrix<'a>) = opencl {
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

    let private pack (firstArray: int[]) (secondArray: int[]) = opencl {
        let length = firstArray.Length

        let kernel =
            <@
                fun (range: _1D)
                    (firstArray: int[])
                    (secondArray: int[])
                    (packed: uint64[]) ->

                    let gid = range.GlobalID0
                    if gid < length then
                        packed.[gid] <- (uint64 firstArray.[gid] <<< 32) ||| (uint64 secondArray.[gid])
            @>

        let packedArray = Array.zeroCreate<uint64> length

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize length, Utils.defaultWorkGroupSize)
            <| firstArray
            <| secondArray
            <| packedArray

        return packedArray
    }

    let private unpack (packedArray: uint64[]) = opencl {
        let length = packedArray.Length

        let kernel =
            <@
                fun (range: _1D)
                    (packedArray: uint64[])
                    (firstArray: int[])
                    (secondArray: int[]) ->

                    let gid = range.GlobalID0
                    if gid < length then
                        firstArray.[gid] <- int ((packedArray.[gid] &&& 0xFFFFFFFF0000000UL) >>> 32)
                        secondArray.[gid] <- int (packedArray.[gid] &&& 0xFFFFFFFUL)
            @>

        let firstArray = Array.zeroCreate<int> length
        let secondArray = Array.zeroCreate<int> length

        do! RunCommand kernel <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize length, Utils.defaultWorkGroupSize)
            <| packedArray
            <| firstArray
            <| secondArray

        return firstArray, secondArray
    }

    let private compressRows rowCount (rowIndices: int[]) = opencl {
        let nnz = rowIndices.Length

        let getUniqueBitmap =
            <@
                fun (ndRange: _1D)
                    (inputArray: int[])
                    (isUniqueBitmap: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < nnz - 1 && inputArray.[i] = inputArray.[i + 1] then
                        isUniqueBitmap.[i] <-    0
            @>

        let bitmap = Array.create nnz 1

        do! RunCommand getUniqueBitmap <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize nnz, Utils.defaultWorkGroupSize)
            <| rowIndices
            <| bitmap

        let! (positions, totalSum) = PrefixSum.runExclude bitmap
        let! _ = ToHost totalSum
        let totalSum = totalSum.[0]

        let calcHyperSparseRows =
            <@
                fun (ndRange: _1D)
                    (rowsIndices: int[])
                    (bitmap: int[])
                    (positions: int[])
                    (nonZeroRowsIndices: int[])
                    (nonZeroRowsPointers: int[]) ->

                    let gid = ndRange.GlobalID0

                    if gid < nnz && bitmap.[gid] = 1 then
                        nonZeroRowsIndices.[positions.[gid]] <- rowsIndices.[gid]
                        nonZeroRowsPointers.[positions.[gid]] <- gid + 1
            @>

        let nonZeroRowsIndices = Array.zeroCreate totalSum
        let nonZeroRowsPointers = Array.zeroCreate totalSum

        do! RunCommand calcHyperSparseRows <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize nnz, Utils.defaultWorkGroupSize)
            <| rowIndices
            <| bitmap
            <| positions
            <| nonZeroRowsIndices
            <| nonZeroRowsPointers

        let calcNnzPerRowSparse =
            <@
                fun (ndRange: _1D)
                    (nonZeroRowsPointers: int[])
                    (nnzPerRowSparse: int[]) ->

                    let gid = ndRange.GlobalID0
                    if gid = 0 then
                        nnzPerRowSparse.[gid] <- nonZeroRowsPointers.[gid]
                    elif gid < totalSum then
                        nnzPerRowSparse.[gid] <- nonZeroRowsPointers.[gid] - nonZeroRowsPointers.[gid - 1]
            @>

        let nnzPerRowSparse = Array.zeroCreate totalSum

        do! RunCommand calcNnzPerRowSparse <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize totalSum, Utils.defaultWorkGroupSize)
            <| nonZeroRowsPointers
            <| nnzPerRowSparse

        let expandSparseNnzPerRow =
            <@
                fun (ndRange: _1D)
                    (nnzPerRowSparse: int[])
                    (nonZeroRowsIndices: int[])
                    (off2: int[]) ->

                    let gid = ndRange.GlobalID0

                    if gid < totalSum then
                        off2.[nonZeroRowsIndices.[gid] + 1] <- nnzPerRowSparse.[gid]
            @>

        let expandedNnzPerRow = Array.zeroCreate (rowCount + 1)

        do! RunCommand expandSparseNnzPerRow <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize totalSum, Utils.defaultWorkGroupSize)
            <| nnzPerRowSparse
            <| nonZeroRowsIndices
            <| expandedNnzPerRow

        let! (rowPointers, _) = PrefixSum.runInclude expandedNnzPerRow
        return rowPointers
    }
