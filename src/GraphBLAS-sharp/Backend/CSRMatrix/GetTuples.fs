namespace GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open Brahma.OpenCL

type internal GetTuples<'a>(matrix: CSRMatrix<'a>) =
    member this.Invoke() = opencl {
        if matrix.Values.Length = 0 then
            return {
                RowIndices = [||]
                ColumnIndices = [||]
                Values = [||]
            }

        else
            let rowCount = matrix.RowCount

            let expandCsrRows =
                <@
                    fun (ndRange: _1D)
                        (rowPointers: int[])
                        (outputRowIndices: int[]) ->

                        let gid = ndRange.GlobalID0

                        if gid < rowCount then
                            for idx = rowPointers.[gid] to rowPointers.[gid + 1] - 1 do
                                outputRowIndices.[idx] <- gid
                @>

            let rowIndices = Array.zeroCreate<int> matrix.Values.Length

            do! RunCommand expandCsrRows <| fun kernelPrepare ->
                kernelPrepare
                <| _1D(rowCount |> Utils.getDefaultGlobalSize, Utils.defaultWorkGroupSize)
                <| matrix.RowPointers
                <| rowIndices

            let! colIndices = Copy.copyArray matrix.ColumnIndices
            let! vals = Copy.copyArray matrix.Values

            return {
                RowIndices = rowIndices
                ColumnIndices = colIndices
                Values = vals
            }
    }
