namespace GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal GetTuples =
    let fromMatrix (matrix: CSRMatrix<'a>) =
        opencl {
            if matrix.Values.Length = 0 then
                return { RowIndices = [||]; ColumnIndices = [||]; Values = [||] }

            else
                let rowCount = matrix.RowCount

                let expandCsrRows =
                    <@ fun (ndRange: Range1D) (rowPointers: int []) (outputRowIndices: int []) ->

                        let gid = ndRange.GlobalID0

                        if gid < rowCount then
                            for idx = rowPointers.[gid] to rowPointers.[gid + 1] - 1 do
                                outputRowIndices.[idx] <- gid @>

                let rowIndices = Array.zeroCreate<int> matrix.Values.Length

                do!
                    runCommand expandCsrRows
                    <| fun kernelPrepare ->
                        kernelPrepare
                        <| Range1D(rowCount |> Utils.getDefaultGlobalSize, Utils.defaultWorkGroupSize)
                        <| matrix.RowPointers
                        <| rowIndices

                let! colIndices = Copy.copyArray matrix.ColumnIndices
                let! vals = Copy.copyArray matrix.Values

                return { RowIndices = rowIndices; ColumnIndices = colIndices; Values = vals }
        }
