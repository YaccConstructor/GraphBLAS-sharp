namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal GetTuples =
    let ofMatrix (matrix: ClCsrMatrix<'a>) = opencl {
        if matrix.Values.Length = 0 then
            return { RowIndices = [||]; ColumnIndices = [||]; Values = [||] }
        else
            let rowCount = matrix.RowCount

            let expandCsrRows =
                <@
                    fun (ndRange: Range1D) (rowPointers: int clarray) (outputRowIndices: int clarray) ->

                        let gid = ndRange.GlobalID0

                        if gid < rowCount then
                            for idx = rowPointers.[gid] to rowPointers.[gid + 1] - 1 do
                                outputRowIndices.[idx] <- gid
                @>

            let! rowIndices = ClArray.alloc<int> matrix.Values.Length

            do! runCommand expandCsrRows <| fun kernelPrepare ->
                kernelPrepare
                <| Range1D(rowCount |> Utils.getDefaultGlobalSize, Utils.defaultWorkGroupSize)
                <| matrix.RowPointers
                <| rowIndices

            let! colIndices = Copy.copyArray matrix.ColumnIndices
            let! vals = Copy.copyArray matrix.Values

            return { RowIndices = rowIndices; ColumnIndices = colIndices; Values = vals }
    }
