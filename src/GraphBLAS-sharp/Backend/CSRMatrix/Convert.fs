namespace GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal rec Convert =
    let fromCoo (matrix: COOMatrix<'a>) =
        opencl {
            if matrix.Values.Length = 0 then
                return
                    { RowCount = matrix.RowCount
                      ColumnCount = matrix.ColumnCount
                      RowPointers = [| 0; 0 |]
                      ColumnIndices = [||]
                      Values = [||] }
            else
                let! compressedRows = compressRows matrix.RowCount matrix.Rows
                failwith "FIX ME!"
                let cols = [||] //let! cols = Copy.copyArray matrix.Columns
                let vals = [||] //let! vals = Copy.copyArray matrix.Values

                return
                    { RowCount = matrix.RowCount
                      ColumnCount = matrix.ColumnCount
                      RowPointers = compressedRows
                      ColumnIndices = cols
                      Values = vals }
        }

    let private compressRows rowCount (rowIndices: int []) =
        opencl {
            let nnz = rowIndices.Length

            let getUniqueBitmap =
                <@ fun (ndRange: Range1D) (inputArray: int []) (isUniqueBitmap: int []) ->

                    let i = ndRange.GlobalID0

                    if i < nnz - 1 && inputArray.[i] = inputArray.[i + 1] then
                        isUniqueBitmap.[i] <- 0 @>

            let bitmap = Array.create nnz 1

            do!
                runCommand getUniqueBitmap
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D(Utils.getDefaultGlobalSize nnz, Utils.defaultWorkGroupSize)
                    <| rowIndices
                    <| bitmap

            failwith "FIX ME! And rewrite."
            //let! (positions, totalSum) = GraphBLAS.FSharp.Backend.ClArray.prefixSumExclude bitmap
            //let! _ = ToHost totalSum
            //let totalSum = totalSum.[0]
            let positions = [||]
            let totalSum = 0

            let calcHyperSparseRows =
                <@ fun (ndRange: Range1D) (rowsIndices: int []) (bitmap: int []) (positions: int []) (nonZeroRowsIndices: int []) (nonZeroRowsPointers: int []) ->

                    let gid = ndRange.GlobalID0

                    if gid < nnz && bitmap.[gid] = 1 then
                        nonZeroRowsIndices.[positions.[gid]] <- rowsIndices.[gid]
                        nonZeroRowsPointers.[positions.[gid]] <- gid + 1 @>

            let nonZeroRowsIndices = Array.zeroCreate totalSum
            let nonZeroRowsPointers = Array.zeroCreate totalSum

            do!
                runCommand calcHyperSparseRows
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D(Utils.getDefaultGlobalSize nnz, Utils.defaultWorkGroupSize)
                    <| rowIndices
                    <| bitmap
                    <| positions
                    <| nonZeroRowsIndices
                    <| nonZeroRowsPointers

            let calcNnzPerRowSparse =
                <@ fun (ndRange: Range1D) (nonZeroRowsPointers: int []) (nnzPerRowSparse: int []) ->

                    let gid = ndRange.GlobalID0

                    if gid = 0 then
                        nnzPerRowSparse.[gid] <- nonZeroRowsPointers.[gid]
                    elif gid < totalSum then
                        nnzPerRowSparse.[gid] <-
                            nonZeroRowsPointers.[gid]
                            - nonZeroRowsPointers.[gid - 1] @>

            let nnzPerRowSparse = Array.zeroCreate totalSum

            do!
                runCommand calcNnzPerRowSparse
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D(Utils.getDefaultGlobalSize totalSum, Utils.defaultWorkGroupSize)
                    <| nonZeroRowsPointers
                    <| nnzPerRowSparse

            let expandSparseNnzPerRow =
                <@ fun (ndRange: Range1D) (nnzPerRowSparse: int []) (nonZeroRowsIndices: int []) (expandedNnzPerRow: int []) ->

                    let gid = ndRange.GlobalID0

                    if gid < totalSum then
                        expandedNnzPerRow.[nonZeroRowsIndices.[gid] + 1] <- nnzPerRowSparse.[gid] @>

            let expandedNnzPerRow = Array.zeroCreate (rowCount + 1)

            do!
                runCommand expandSparseNnzPerRow
                <| fun kernelPrepare ->
                    kernelPrepare
                    <| Range1D(Utils.getDefaultGlobalSize totalSum, Utils.defaultWorkGroupSize)
                    <| nnzPerRowSparse
                    <| nonZeroRowsIndices
                    <| expandedNnzPerRow

            failwith "FIX ME! And rewrite."
            //let! (rowPointers, _) = PrefixSum.runInclude expandedNnzPerRow
            //return rowPointers
            return [||]
        }
