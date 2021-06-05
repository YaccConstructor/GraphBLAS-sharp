namespace GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open Brahma.OpenCL

module internal rec Convert =
    let fromCoo (matrix: COOMatrix<'a>) = opencl {
        if matrix.Values.Length = 0 then
            return {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                RowPointers = [| 0; 0 |]
                ColumnIndices = [||]
                Values = [||]
            }
        else
            let! compressedRows = compressRows matrix.RowCount matrix.Rows
            let! cols = Copy.copyArray matrix.Columns
            let! vals = Copy.copyArray matrix.Values

            return {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                RowPointers = compressedRows
                ColumnIndices = cols
                Values = vals
            }
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
                    (expandedNnzPerRow: int[]) ->

                    let gid = ndRange.GlobalID0

                    if gid < totalSum then
                        expandedNnzPerRow.[nonZeroRowsIndices.[gid] + 1] <- nnzPerRowSparse.[gid]
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
